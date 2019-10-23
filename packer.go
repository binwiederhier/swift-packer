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
	"sync/atomic"
	"time"
)

type Packer interface {
	ListenAndServe() error
}

type Config struct {
	ListenAddr  string
	ForwardAddr string
	MinSize     int
	MaxWait     time.Duration
}

type packer struct {
	config  *Config
	client  *http.Client
	clients int32
	buffers *sync.Pool
	packs   map[string]*pack
	sync.Mutex
}

type xpart struct {
	data io.Reader
}
type pack struct {
	groupId string
	packId string

	first     *http.Request
	parts     map[string]*xpart
	size      int

	timer     *time.Timer
	responses chan *http.Response
	done      chan bool
}

func NewPacker(config *Config) Packer {
	return &packer{
		config:  config,
		client:  &http.Client{},
		clients: 0,
		buffers: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, config.MinSize)
				return b
			},
		},
		packs: make(map[string]*pack),
	}
}

func (p *packer) ListenAndServe() error {
	rand.Seed(time.Now().UnixNano())

	server := &http.Server{
		Addr:    p.config.ListenAddr,
		Handler: p,
	}

	go func() {
		for {
			log.Printf("Clients %d\n", atomic.LoadInt32(&p.clients))
			time.Sleep(1 * time.Second)
		}
	}()

	return server.ListenAndServe()
}

func (p *packer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&p.clients, 1)
	defer atomic.AddInt32(&p.clients, -1)

	if r.Method == "PUT" && r.Header.Get("X-Pack") == "yes" {
		p.handlePut(w, r)
	} else {
		p.forwardRequest(w, r)
	}
}

func (p *packer) handlePut(w http.ResponseWriter, request *http.Request) {
	// Read up to buffer size into memory
	buffer := p.buffers.Get().([]byte)
	defer p.buffers.Put(buffer)

	off := 0
	for off < len(buffer) {
		read, err := request.Body.Read(buffer[off:])
		if err == io.EOF {
			off += read
			break
		} else if err != nil {
			request.Body.Close()
			w.WriteHeader(500)
			return
		}

		off += read
	}

	// Request is at least as large as buffer, forward upstream
	if off == len(buffer) {
		debugf("handlePut - buffer full (%d bytes), forwarding\n", len(buffer))
		request.Body = ioutil.NopCloser(io.MultiReader(bytes.NewReader(buffer[:off]), request.Body)) // FIXME leaking body
		p.forwardRequest(w, request)
		return
	}

	if err := request.Body.Close(); err != nil {
		panic(err)
	}

	debugf("Incoming request %s fully read (%d bytes), appending to pack\n", request.URL.Path, off)

	// Determine pack group
	var groupId string
	var err error

	if request.Header.Get("X-Pack-Group") != "" {
		groupId = request.Header.Get("X-Pack-Group")
	} else {
		groupId, err = parsePrefix(request.URL.Path)
		if err != nil {
			p.fail(w, 400, err)
			return
		}
	}

	p.Lock()
	apack, ok := p.packs[groupId]
	if !ok {
		apack = p.newPack(groupId, request)
		p.packs[groupId] = apack
	}

	apack.parts[request.URL.Path] = &xpart{
		data: bytes.NewReader(buffer[:off]),
	}

	apack.size += len(buffer[:off])
	apack.timer.Reset(p.config.MaxWait)

	if apack.size > p.config.MinSize {
		go p.packUpload(apack)
		delete(p.packs, groupId)
		apack.done <- true
	}
	p.Unlock()

	response := <-apack.responses
	debugf("group %s - pack n/a    - handlePut - request %s - response received: %s\n",
		groupId, request.URL.Path, response.Status)

	copyAndWriteResponseHeader(w, response)
	if err := writeResponse(w, response); err != nil {
		debugf("Cannot write response: " + err.Error())
	}
}

func (p *packer) newPack(groupId string, first *http.Request) *pack {
	apack := &pack{
		groupId:   groupId,
		packId:    randomHexChars(32),

		first:     first,
		parts:     make(map[string]*xpart, 0),
		size:      0,

		timer:     time.NewTimer(p.config.MaxWait),
		responses: make(chan *http.Response),
		done:      make(chan bool),
	}

	go func() {
		select {
		case <-apack.timer.C:
			p.Lock()
			go p.packUpload(apack)
			delete(p.packs, groupId)
			p.Unlock()
			break
		case <-apack.done:
			break
		}
	}()

	return apack
}

func (p *packer) packUpload(apack *pack) {
	prefix, err := parsePrefix(apack.first.URL.Path)
	if err != nil {
		panic(err)
	}

	uri := fmt.Sprintf("http://%s/%s/%s", p.config.ForwardAddr, prefix, apack.packId)
	debugf("Uploading pack %s (group %s) to %s\n", apack.packId, apack.groupId, uri)

	readers := make([]io.Reader, 0)
	for _, part := range apack.parts {
		readers = append(readers, part.data)
	}

	upstreamRequest, err := http.NewRequest("PUT", uri, io.MultiReader(readers...))
	if err != nil {
		panic(err)
	}

	copyRequestHeader(upstreamRequest, apack.first)
	response, err := p.client.Do(upstreamRequest)
	if err != nil {
		panic(err)
	}

	debugf("group %s - pack %s - handlePack - Upload finished, len %d, count %d, status %s\n", apack.groupId,
		apack.packId, apack.size, len(apack.parts), response.Status)

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
	for i := 0; i < len(apack.parts); i++ {
		downResponse := response
		if body != nil {
			downResponse.Body = ioutil.NopCloser(bytes.NewReader(body))
		}
		apack.responses <- downResponse
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
