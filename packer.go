package packer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
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
	log.Printf("ServeHTTP: received request %s (group %s)\n", request.URL.Path, groupId)

	group.requests <- request
	log.Printf("ServeHTTP: dispatched request %s (group %s)\n", request.URL.Path, groupId)
	response := <-group.responses
	log.Printf("ServeHTTP: response received for %s: %s (group %s)\n", request.URL.Path, response.Status, groupId)

	copyAndWriteResponseHeader(w, response)
	if err := writeResponse(w, response); err != nil {
		log.Printf("Cannot write response: " + err.Error())
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
		log.Printf("Cannot write response to forwarded request: " + err.Error())
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
		group = newPackGroup(p.config, p.client)
		p.groups[groupId] = group
		go func() {
			group.requestHandler()
			p.Lock()
			delete(p.groups, groupId)
			p.Unlock()
		}()
	}

	return group
}

type packGroup struct {
	config    *Config
	client    *http.Client
	requests  chan *http.Request
	responses chan *http.Response
}

func newPackGroup(config *Config, client *http.Client) *packGroup {
	return &packGroup{
		config:    config,
		client:    client,
		requests:  make(chan *http.Request),
		responses: make(chan *http.Response),
	}
}

func (g *packGroup) requestHandler() {
	log.Printf("Dispatch: started pack dispatch\n")
	var pack *pack
	var err error
	var more bool
	var request *http.Request

	for {
		select {
		case request = <-g.requests:
			break
		case <-time.After(time.Duration(g.config.MaxWait) * time.Millisecond):
			if pack != nil {
				log.Printf("Dispatch: pack %s timed out\n", pack.id)
				pack.requests <- nil
				return
			}
		}

		if pack == nil {
			pack, err = newPack(g.config, request)
			if err != nil {
				log.Printf("Dispatch: Error creating new pack: %s\n", err.Error())
				g.responses <- &http.Response{StatusCode:500}
				continue // TODO handle errors properly!
			}

			go g.handlePack(pack) // TODO handle errors!
		}

		pack.requests <- request
		more = <-pack.more

		log.Printf("Dispatch: pack %s more receied: %t\n", pack.id, more)
		if !more {
			log.Printf("Dispatch: pack %s is full\n", pack.id)
			pack.requests <- nil
			pack = nil
		}
	}
}

func (g *packGroup) handlePack(apack *pack) {
	uri := fmt.Sprintf("http://%s/%s/%s", g.config.ForwardAddr, apack.prefix, apack.id)
	log.Printf("handlePack: pack %s -> uploading to %s\n", apack.id, uri)

	request, err := http.NewRequest("PUT", uri, apack)
	if err != nil {
		panic(err)
	}

	copyRequestHeader(request, apack.request)
	response, err := g.client.Do(request)
	if err != nil {
		panic(err)
	}

	log.Printf("handlePack: pack %s uploaded, len %d, count %d, status %s\n", apack.id, apack.length, apack.count, response.Status)

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
	config *Config
	prefix string
	id     string

	request  *http.Request
	requests chan *http.Request
	more     chan bool

	current *http.Request
	length  int
	count   int

	sync.Mutex
}

func newPack(config *Config, request *http.Request) (*pack, error) {
	prefix, err := parsePrefix(request.URL.Path)
	if err != nil {
		return nil, err
	}

	return &pack{
		config: config,
		prefix: prefix,
		id:     randomHexChars(32),

		request:  request,
		requests: make(chan *http.Request),
		more:     make(chan bool),

		current:  nil,
		length:   0,
		count:    0,
	}, nil
}

func (p *pack) Read(out []byte) (int, error) {
	p.Lock()
	defer p.Unlock()

	log.Printf("pack %s - read(%d)\n", p.id, len(out))
	if p.current == nil {
		p.current = <-p.requests
		if p.current == nil {
			log.Printf("Read: pack %s -> nil request received. Closing pack\n", p.id)
			return 0, io.EOF
		}
	}

	off := 0
	for off < len(out) {
		read, err := p.current.Body.Read(out[off:])
		if err == io.EOF {
			err = p.current.Body.Close()
			if err != nil {
				p.more <- false
				return 0, err
			}

			off += read
			p.length += read
			p.current = nil
			break
		} else if err != nil {
			p.more <- false
			return 0, err
		}

		off += read
		p.length += read
	}

	p.count++
	log.Printf("pack %s - read %d, total pack len %d, count %d\n", p.id, off, p.length, p.count)

	if p.length > p.config.MinSize {
		p.more <- false
	} else {
		p.more <- true
	}

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
