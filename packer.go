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

type part struct {
	response chan *http.Response
	offset   int
	data     []byte
}

type pack struct {
	groupId string
	packId string

	firstRequest *http.Request
	parts        []*part
	size         int

	timer     *time.Timer
	done      chan bool
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

	read, err := p.readBuffer(buffer, request.Body)
	if err != nil {
		request.Body.Close()
		p.fail(w, 500, err)
		return
	}

	// Request is at least as large as buffer, forward upstream
	if read == len(buffer) {
		debugf("Incoming request %s too large for packing (%d bytes), forwarding upstream\n", request.URL.Path, read)
		request.Body = ioutil.NopCloser(io.MultiReader(bytes.NewReader(buffer[:read]), request.Body)) // FIXME leaking body
		p.forwardRequest(w, request)
		return
	}

	// Request is fully in the buffer now, close request body and start packing
	if err := request.Body.Close(); err != nil {
		p.fail(w, 500, err)
		return
	}

	debugf("Incoming request %s fully read (%d bytes), appending to pack\n", request.URL.Path, read)

	// Determine pack group
	groupId, err := p.parseGroupId(request)
	if err != nil {
		p.fail(w, 400, err)
		return
	}

	p.Lock()
	apack, ok := p.packs[groupId]
	if !ok {
		apack = p.newPack(groupId, request)
		p.packs[groupId] = apack
	}

	responseChan := make(chan *http.Response)
	defer close(responseChan)

	apack.parts = append(apack.parts, &part{
		offset: apack.size,
		data: buffer[:read],
		response: responseChan,
	})
	apack.size += len(buffer[:read]) // Do not reorder, see above!
	apack.timer.Reset(p.config.MaxWait)

	if apack.size > p.config.MinSize {
		delete(p.packs, groupId)
		apack.done <- true
	}
	p.Unlock()

	response := <-responseChan
	debugf("Dispatching response for pack: %s (group %s), request: %s, response: %s\n",
		apack.packId, apack.groupId, request.URL.Path, response.Status)

	copyAndWriteResponseHeader(w, response)
	if err := writeResponse(w, response); err != nil {
		debugf("Cannot write response: " + err.Error())
	}
}

func (p *packer) newPack(groupId string, first *http.Request) *pack {
	apack := &pack{
		groupId:   groupId,
		packId:    randomHexChars(32),

		firstRequest: first,
		parts:        make([]*part, 0),
		size:         0,

		timer:     time.NewTimer(p.config.MaxWait),
		done:      make(chan bool),
	}

	go func() {
		select {
		case <-apack.timer.C:
			p.Lock()
			delete(p.packs, groupId)
			p.Unlock()
			break
		case <-apack.done:
			break
		}

		go p.packUpload(apack)
	}()

	return apack
}

func (p *packer) packUpload(apack *pack) {
	prefix, err := parsePrefix(apack.firstRequest.URL.Path)
	if err != nil {
		panic(err)
	}

	uri := fmt.Sprintf("http://%s/%s/%s", p.config.ForwardAddr, prefix, apack.packId)
	debugf("Uploading pack %s (group %s) to %s\n", apack.packId, apack.groupId, uri)

	readers := make([]io.Reader, 0)
	for _, apart := range apack.parts {
		readers = append(readers, bytes.NewReader(apart.data))
	}

	upstreamRequest, err := http.NewRequest("PUT", uri, io.MultiReader(readers...))
	if err != nil {
		panic(err)
	}

	copyRequestHeader(upstreamRequest, apack.firstRequest)
	upstreamResponse, err := p.client.Do(upstreamRequest)
	if err != nil {
		panic(err)
	}

	debugf("Uploading pack %s (group %s) finished, len %d, count %d, status %s\n",
		apack.packId, apack.groupId, apack.size, len(apack.parts), upstreamResponse.Status)

	var body []byte
	if upstreamResponse.Body != nil {
		body, err = ioutil.ReadAll(upstreamResponse.Body)
		if err != nil {
			panic(err)
		}
		if err := upstreamResponse.Body.Close(); err != nil {
			panic(err)
		}
	}

	for _, apart := range apack.parts {
		response := &http.Response{
			Status: upstreamResponse.Status,
			StatusCode: upstreamResponse.StatusCode,
			Header: copyHeader(upstreamResponse.Header),
		}

		response.Header.Add("X-Pack-Id", apack.packId)
		response.Header.Add("X-Item-Offset", fmt.Sprintf("%d", apart.offset))
		response.Header.Add("X-Item-Length", fmt.Sprintf("%d", len(apart.data)))

		if body != nil {
			response.Body = ioutil.NopCloser(bytes.NewReader(body))
		}

		apart.response <- response
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
		p.fail(w, 500, err)
		return
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

func copyHeader(from http.Header) http.Header {
	to := http.Header{}
	for header, values := range from {
		for _, value := range values {
			to.Add(header, value)
		}
	}
	return to
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
