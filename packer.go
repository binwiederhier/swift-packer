package packer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"reflect"
	"regexp"
	"sync"
	"time"
)

type Packer interface {
	ListenAndServe() error
}

type Config struct {
	ListenAddr  string
	ForwardAddr string
	MinSize     int
	MaxWait     int
}

type packer struct {
	config *Config
	client *http.Client
	groups map[string]*packGroup
	sync.Mutex
}

func NewPacker(config *Config) Packer {
	return &packer{
		config: config,
		client: &http.Client{},
		groups: make(map[string]*packGroup, 0),
	}
}

func (p *packer) ListenAndServe() error {
	rand.Seed(time.Now().UnixNano())

	server := &http.Server{
		Addr:    p.config.ListenAddr,
		Handler: p,
	}

	return server.ListenAndServe()
}

func (p *packer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "PUT" && r.Header.Get("X-Pack") == "yes" {
		p.handlePut(w, r)
	} else {
		p.forwardRequest(w, r)
	}
}

func (p *packer) handlePut(w http.ResponseWriter, request *http.Request) {
	var groupId string
	var err error

	// Determine pack group
	if request.Header.Get("X-Pack-Group") != "" {
		groupId = request.Header.Get("X-Pack-Group")
	} else {
		groupId, err = parsePrefix(request.URL.Path)
		if err != nil {
			p.fail(w, 400, err)
			return
		}
	}

	// Fetch group and queue request
	group := p.group(groupId)
	debugf("group %s - pack n/a    - handlePut - request %s - received\n", groupId, request.URL.Path)

	group.requests <- request
	debugf("group %s - pack n/a    - handlePut - request %s - dispatched\n", groupId, request.URL.Path)
	response := <-group.responses
	debugf("group %s - pack n/a    - handlePut - request %s - response received: %s\n",
		groupId, request.URL.Path, response.Status)

	copyAndWriteResponseHeader(w, response)
	if err := writeResponse(w, response); err != nil {
		debugf("Cannot write response: " + err.Error())
	}
}

func (p *packer) forwardRequest(w http.ResponseWriter, r *http.Request) {
	forwardURL := r.URL
	forwardURL.Scheme = "http"
	forwardURL.Host = p.config.ForwardAddr

	proxyRequest, err := http.NewRequest(r.Method, forwardURL.String(), r.Body)
	if err != nil {
		p.fail(w, 500, err)
		return
	}

	copyRequestHeader(proxyRequest, r)
	proxyRequest.Header.Set("Host", r.Host)

	proxyResponse, err := p.client.Do(proxyRequest)
	if err != nil {
		panic(err)
	}

	copyAndWriteResponseHeader(w, proxyResponse)
	if err := writeResponse(w, proxyResponse); err != nil {
		debugf("Cannot write response to forwarded request: " + err.Error())
	}
}

func (p *packer) fail(w http.ResponseWriter, status int, err error) {
	w.WriteHeader(status)
	w.Write([]byte(err.Error()))
}

func (p *packer) group(groupId string) *packGroup {
	p.Lock()
	defer p.Unlock()

	group, ok := p.groups[groupId]
	if !ok {
		debugf("group %s - group - NEW\n", groupId)
		group = newPackGroup(groupId, p.config, p.client)
		p.groups[groupId] = group
		go func() {
			group.groupWorker()
			p.Lock()
			delete(p.groups, groupId)
			p.Unlock()
		}()
	}

	return group
}

type packGroup struct {
	groupId   string
	config    *Config
	client    *http.Client
	requests  chan *http.Request
	responses chan *http.Response
}

func newPackGroup(groupId string, config *Config, client *http.Client) *packGroup {
	return &packGroup{
		groupId:   groupId,
		config:    config,
		client:    client,
		requests:  make(chan *http.Request),
		responses: make(chan *http.Response),
	}
}

func (g *packGroup) groupWorker() {
	debugf("group %s - pack n/a    - groupWorker - STARTED\n", g.groupId)

	workers := make([]chan *http.Request, 0)
	for i := 0; i < 1; i++ {
		workers = append(workers, make(chan *http.Request))
		go g.packWorker(workers[i])
	}

	for {
		select {
		case request := <-g.requests:
			debugf("group %s - pack n/a    - groupWorker - Read request %s\n", g.groupId, request.URL.Path)
			workers[rand.Intn(len(workers))] <- request
			break
		case <-time.After(time.Duration(g.config.MaxWait) * time.Millisecond):
			debugf("group %s - pack n/a    - groupWorker - Timeout. Sending NIL to workers\n", g.groupId)
			for _, worker := range workers {
				worker <- nil
			}
			debugf("group %s - pack n/a    - groupWorker - EXITED\n", g.groupId)
			return
		}
	}
}

// AWFUL
func randMapKey(m interface{}) interface{} {
	mapKeys := reflect.ValueOf(m).MapKeys()
	return mapKeys[rand.Intn(len(mapKeys))].Interface()
}

func (g *packGroup) packWorker(requests chan *http.Request) {
	var err error
	var more bool
	var apack *pack

	for request := range requests {
		if request == nil {
			debugf("group %s - pack n/a    - packWork - NIL received. Closing packWorker\n", g.groupId)
			close(requests)
			return
		}

		if apack == nil {
			debugf("group %s - pack n/a    - packWork - Creating NEW PACK\n", g.groupId)
			apack, err = newPack(g.groupId, g.config, request)
			if err != nil {
				debugf("group %s - pack n/a    - packWork - ERROR creating pack: %s\n", g.groupId, err.Error())
				g.responses <- &http.Response{StatusCode: 500}
				continue // TODO handle errors properly!
			}

			go g.handlePack(apack) // TODO handle errors!
		}

		debugf("group %s - pack %s - packWork - request %s - Sending\n", g.groupId, apack.id, request.URL.Path)
		apack.requests <- request
		debugf("group %s - pack %s - packWork - request %s - Waiting for more\n", g.groupId, apack.id, request.URL.Path)
		more = <-apack.more
		debugf("group %s - pack %s - packWork - request %s - More received = %t\n", g.groupId, apack.id, request.URL.Path, more)

		if !more {
			debugf("group %s - pack %s - packWork - request %s - Pack FULL, sending 'nil'\n", g.groupId, apack.id, request.URL.Path)
			apack.requests <- nil
			apack = nil
		}
	}

	debugf("group %s - pack n/a    - packWork - Exiting\n", g.groupId)
}

func (g *packGroup) handlePack(apack *pack) {
	uri := fmt.Sprintf("http://%s/%s/%s", g.config.ForwardAddr, apack.prefix, apack.id)
	debugf("group %s - pack %s - handlePack - Starting upload to %s\n", g.groupId, apack.id, uri)

	request, err := http.NewRequest("PUT", uri, apack)
	if err != nil {
		panic(err)
	}

	copyRequestHeader(request, apack.request)
	response, err := g.client.Do(request)
	if err != nil {
		panic(err)
	}

	debugf("group %s - pack %s - handlePack - Upload finished, len %d, count %d, status %s\n", g.groupId,
		apack.id, apack.length, apack.count, response.Status)

	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		if err != nil {
			panic(err)
		}
		if err := response.Body.Close(); err != nil {
			panic(err)
		}
	}

	response.Header.Add("X-Pack-Id", uri)
	for i := 0; i < apack.count; i++ {
		downResponse := response
		if body != nil {
			downResponse.Body = ioutil.NopCloser(bytes.NewReader(body))
		}
		g.responses <- downResponse
	}
}

type pack struct {
	groupId string
	id      string
	config  *Config
	prefix  string

	request  *http.Request
	requests chan *http.Request
	more     chan bool

	current *http.Request
	length  int
	count   int

	sync.Mutex
}

func newPack(groupId string, config *Config, request *http.Request) (*pack, error) {
	prefix, err := parsePrefix(request.URL.Path)
	if err != nil {
		return nil, err
	}

	return &pack{
		groupId: groupId,
		id:      randomHexChars(6),
		config:  config,
		prefix:  prefix,

		request:  request,
		requests: make(chan *http.Request),
		more:     make(chan bool),

		current:  nil,
		length:   0,
		count:    0,
	}, nil
}

func (p *pack) Read(out []byte) (int, error) {
	//p.Lock()
	//defer p.Unlock()

	debugf("group %s - pack %s - Read       - len = %d\n", p.groupId, p.id, len(out))
	if p.current == nil {
		p.current = <-p.requests
		if p.current == nil {
			debugf("group %s - pack %s - Read       - -> nil request received. Closing pack\n", p.groupId, p.id)
			return 0, io.EOF
		}

		p.count++
	}

	off := 0
	for off < len(out) {
		debugf("group %s - pack %s - Read       - Loop len = %d, off = %d\n", p.groupId, p.id, len(out), off)
		read, err := p.current.Body.Read(out[off:])
		if err == io.EOF {
			debugf("group %s - pack %s - Read       - EOF\n", p.groupId, p.id)
			err = p.current.Body.Close()
			if err != nil {
				debugf("group %s - pack %s - Read       - ERROR on close: %s\n", p.groupId, p.id, err.Error())
				p.more <- false
				return 0, err
			}

			off += read
			p.length += read
			p.current = nil

			if p.length > p.config.MinSize {
				//debugf("group %s - pack %s - Read       - Sending more = %t \n", p.groupId, p.id, false)
				p.more <- false
			} else {
				//debugf("group %s - pack %s - Read       - Sending more = %t \n", p.groupId, p.id, true)
				p.more <- true
			}

			break
		} else if err != nil {
			debugf("group %s - pack %s - Read       - ERROR on read: %s\n", p.groupId, p.id, err.Error())
			p.more <- false
			return 0, err
		}

		off += read
		p.length += read
	}

	debugf("group %s - pack %s - Read       - RETURN, off %d, total pack len %d, count %d\n", p.groupId, p.id, off, p.length, p.count)

	return off, nil
}

var hexCharset = []rune("0123456789abcdef")
var prefixRegex = regexp.MustCompile(`^/(v1/[^/]+/[^/]+)/.+$`)

func randomHexChars(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = hexCharset[rand.Intn(len(hexCharset))]
	}
	return string(b)
}

func parsePrefix(path string) (prefix string, err error) {
	matches := prefixRegex.FindStringSubmatch(path)
	if matches == nil || len(matches) != 2 {
		return "", errors.New("unable to parse path into account/container")
	}

	return matches[1], nil
}

func copyRequestHeader(to *http.Request, from *http.Request) {
	for header, values := range from.Header {
		for _, value := range values {
			to.Header.Add(header, value)
		}
	}
}

func copyAndWriteResponseHeader(w http.ResponseWriter, from *http.Response) {
	for header, values := range from.Header {
		for _, value := range values {
			w.Header().Add(header, value)
		}
	}

	w.WriteHeader(from.StatusCode)
}

func writeResponse(w http.ResponseWriter, response *http.Response) error {
	if _, err := io.Copy(w, response.Body); err != nil {
		return err
	}

	if err := response.Body.Close(); err != nil {
		return err
	}

	return nil
}
