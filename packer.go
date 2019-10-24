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

type pack struct {
	config  *Config
	groupId string
	packId  string

	first *http.Request
	parts []*part
	size  int
	full  bool

	timer *time.Timer
	done  chan bool
}

type part struct {
	response chan<- *http.Response
	offset   int
	data     []byte
}

var (
	hexCharset = []rune("0123456789abcdef")
	prefixRegex = regexp.MustCompile(`^/(v1/[^/]+/[^/]+)/.+$`)
)

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

	if r.Method == http.MethodPut && r.Header.Get("X-Pack") == "yes" {
		p.handlePut(w, r)
	} else {
		p.forwardRequest(w, r)
	}
}

func (p *packer) handlePut(w http.ResponseWriter, request *http.Request) {
	// Read up to buffer size into memory
	buffer := p.buffers.Get().([]byte)
	defer p.buffers.Put(buffer)

	read, err := p.readBuffer(buffer, request.Body)
	if err != nil {
		request.Body.Close() // Ignore error!
		http.Error(w, err.Error(), 500)
		return
	}

	// Request is at least as large as buffer, forward upstream
	if read == len(buffer) {
		debugf("Incoming request %s too large for packing (> %d bytes), forwarding upstream\n", request.URL.Path, read)
		request.Body = ioutil.NopCloser(io.MultiReader(bytes.NewReader(buffer[:read]), request.Body)) // FIXME leaking body
		p.forwardRequest(w, request)
		return
	}

	// Request is fully in the buffer now, close request body and start packing
	if err := request.Body.Close(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	debugf("Incoming request %s fully read (%d bytes), appending to pack\n", request.URL.Path, read)

	// Determine pack group
	groupId, err := p.parseGroupId(request)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Find pack, append part to it
	p.Lock()
	apack := p.createOrGetPack(groupId, request)

	responseChan := make(chan *http.Response)
	defer close(responseChan)

	p.appendPart(apack, buffer[:read], responseChan)
	if apack.full {
		p.deletePack(apack.groupId)
		apack.done <- true
	}
	p.Unlock()

	// Wait for response from packUpload
	response := <-responseChan

	debugf("Dispatching response for pack: %s (group %s), request: %s, response: %s\n",
		apack.packId, apack.groupId, request.URL.Path, response.Status)

	if err := writeResponse(w, response); err != nil {
		debugf("Cannot write response: " + err.Error())
	}
}

func (p *packer) createOrGetPack(groupId string, first *http.Request) *pack {
	apack, ok := p.packs[groupId]
	if ok {
		return apack
	}

	apack = &pack{
		config:  p.config,
		groupId: groupId,
		packId:  randomHexChars(32),

		first: first,
		parts: make([]*part, 0),
		size:  0,
		full:  false,

		timer:     time.NewTimer(p.config.MaxWait),
		done:      make(chan bool),
	}

	go func() {
		select {
		case <-apack.timer.C:
			p.Lock()
			p.deletePack(groupId)
			p.Unlock()
			break
		case <-apack.done:
			break
		}

		go p.packWorker(apack)
	}()

	p.packs[groupId] = apack
	return apack
}

func (p *packer) deletePack(groupId string) {
	delete(p.packs, groupId)
}

func (p *packer) packWorker(apack *pack) {
	response, err := p.packUpload(apack)
	if err != nil {
		response = &http.Response{
			StatusCode: 500,
			Status: "500 Internal Server Error",
		}
	}

	p.dispatchResponses(apack, response)
}

func (p *packer) packUpload(apack *pack) (*http.Response, error) {
	prefix, err := parsePrefix(apack.first.URL.Path)
	if err != nil {
		return nil, err
	}

	uri := fmt.Sprintf("http://%s/%s/%s", p.config.ForwardAddr, prefix, apack.packId)
	debugf("Uploading pack %s (group %s) to %s\n", apack.packId, apack.groupId, uri)

	readers := make([]io.Reader, 0)
	for _, apart := range apack.parts {
		readers = append(readers, bytes.NewReader(apart.data))
	}

	upstreamRequest, err := http.NewRequest("PUT", uri, io.MultiReader(readers...))
	if err != nil {
		return nil, err
	}

	upstreamRequest.Header = apack.first.Header.Clone()
	upstreamResponse, err := p.client.Do(upstreamRequest)
	if err != nil {
		return nil, err
	}

	debugf("Finished uploading pack %s (group %s), len %d, count %d, status %s\n",
		apack.packId, apack.groupId, apack.size, len(apack.parts), upstreamResponse.Status)

	return upstreamResponse, nil
}

func (p *packer) dispatchResponses(apack *pack, response *http.Response) {
	var body []byte
	var err error

	// Read upstream response body
	if response.Body != nil {
		if body, err = ioutil.ReadAll(response.Body); err != nil {
			body = []byte(err.Error())
		}
		response.Body.Close() // Ignore errors!
	}

	// Dispatch response to downstream
	for _, apart := range apack.parts {
		partResponse := &http.Response{
			Status:     response.Status,
			StatusCode: response.StatusCode,
			Header:     response.Header.Clone(),
		}

		partResponse.Header.Add("X-Pack-Id", apack.packId)
		partResponse.Header.Add("X-Item-Offset", fmt.Sprintf("%d", apart.offset))
		partResponse.Header.Add("X-Item-Length", fmt.Sprintf("%d", len(apart.data)))

		if body != nil {
			partResponse.Body = ioutil.NopCloser(bytes.NewReader(body))
		}

		apart.response <- partResponse
	}
}

func (p *packer) forwardRequest(w http.ResponseWriter, request *http.Request) {
	forwardURL := request.URL
	forwardURL.Scheme = "http"
	forwardURL.Host = p.config.ForwardAddr

	proxyRequest, err := http.NewRequest(request.Method, forwardURL.String(), request.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	proxyRequest.Header = request.Header.Clone()
	proxyRequest.Header.Set("Host", request.Host)

	proxyResponse, err := p.client.Do(proxyRequest)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if err := writeResponse(w, proxyResponse); err != nil {
		debugf("Cannot write response to forwarded request: " + err.Error())
	}
}

func (p *packer) appendPart(apack *pack, data []byte, responseChan chan<- *http.Response) {
	apack.parts = append(apack.parts, &part{
		offset: apack.size,
		data: data,
		response: responseChan,
	})
	apack.size += len(data) // Do not reorder, see above!
	apack.timer.Reset(p.config.MaxWait)
	apack.full = apack.size > p.config.MinSize
}

func (p *packer) parseGroupId(request *http.Request) (string, error) {
	if request.Header.Get("X-Pack-Group") != "" {
		return request.Header.Get("X-Pack-Group"), nil
	} else {
		groupId, err := parsePrefix(request.URL.Path)
		if err != nil {
			return "", err
		}
		return groupId, nil
	}
}

func (p *packer) readBuffer(buffer []byte, reader io.ReadCloser) (int, error) {
	off := 0
	for off < len(buffer) {
		read, err := reader.Read(buffer[off:])
		if err == io.EOF {
			off += read
			break
		} else if err != nil {
			return 0, err
		}
		off += read
	}
	return off, nil
}

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

func writeResponse(w http.ResponseWriter, response *http.Response) error {
	// Write headers
	for header, values := range response.Header {
		for _, value := range values {
			w.Header().Add(header, value)
		}
	}

	w.WriteHeader(response.StatusCode)

	// Write and close body
	if _, err := io.Copy(w, response.Body); err != nil {
		return err
	}

	if err := response.Body.Close(); err != nil {
		return err
	}

	return nil
}
