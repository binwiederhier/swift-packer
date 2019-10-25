package packer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Packer interface {
	ListenAndServe() error
}

type Config struct {
	ForwardAddr string
	ListenAddr  string
	MinSize     int
	MaxWait     time.Duration
	MaxCount    int
	Prefix      string
}

type packer struct {
	config  *Config
	client  *http.Client
	clients int32
	buffers *sync.Pool
	packs   map[string]*pack
	sync.Mutex
}

var (
	hexCharset                  = []rune("0123456789abcdef")
	accountContainerObjectRegex = regexp.MustCompile(`^/v1/([^/]+)/([^/]+)/(.+)$`)
	packItemRegex               = regexp.MustCompile(`^([^/]+)/([^/]+)/(\d+)$`)
)

func NewPacker(config *Config) (Packer, error) {
	newConfig, err := copyWithDefaults(config)
	if err != nil {
		return nil, err
	}

	//Debug = true
	return &packer{
		config:  newConfig,
		client:  &http.Client{},
		clients: 0,
		buffers: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, config.MinSize)
				return b
			},
		},
		packs: make(map[string]*pack),
	}, nil
}

func (p *packer) ListenAndServe() error {
	rand.Seed(time.Now().UnixNano())

	server := &http.Server{
		Addr:    p.config.ListenAddr,
		Handler: p,
	}

	go func() {
		for {
			//log.Printf("Clients %d\n", atomic.LoadInt32(&p.clients))
			time.Sleep(1 * time.Second)
		}
	}()

	return server.ListenAndServe()
}

func (p *packer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&p.clients, 1)
	defer atomic.AddInt32(&p.clients, -1)

	if r.Method == http.MethodPut && r.Header.Get("X-Allow-Pack") == "yes" {
		p.handlePUT(w, r)
	} else if r.Method == http.MethodGet {
		p.handleGET(w, r)
	} else {
		p.forwardRequest(w, r)
	}
}

func (p *packer) handlePUT(w http.ResponseWriter, request *http.Request) {
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
	apack, err := p.createOrGetPack(groupId, request)

	responseChan := make(chan *http.Response)
	defer close(responseChan)

	apack.append(buffer[:read], responseChan)
	if apack.full() {
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

func (p *packer) handleGET(w http.ResponseWriter, r *http.Request) {
	account, container, object, err := parseRequestParts(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	packItemParts := packItemRegex.FindStringSubmatch(object)
	if len(packItemParts) != 4 || packItemParts[1] != p.config.Prefix {
		p.forwardRequest(w, r)
		return
	}

	packId := packItemParts[2]
	itemId, err := strconv.Atoi(packItemParts[3])
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	packUri := fmt.Sprintf("http://%s/v1/%s/%s/%s/%s", p.config.ForwardAddr, account, container, p.config.Prefix, packId)
	headRequest, err := http.NewRequest(http.MethodHead, packUri, nil)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	headRequest.Header = r.Header.Clone()
	headResponse, err := p.client.Do(headRequest)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	meta := headResponse.Header.Get("X-Object-Meta-Pack")
	if meta == "" {
		http.Error(w, "Pack file does not contain X-Object-Meta-Pack header", 500)
		return
	}

	itemRange := ""
	packMetaParts := strings.Split(meta, ",")
	for currentItemId, currentItemRange := range packMetaParts {
		if currentItemId == itemId && currentItemRange != "" {
			itemRange = currentItemRange
			break
		}
	}

	if itemRange == "" {
		http.Error(w, "Requested item does not exist in pack", 404)
		return
	}

	getRequest, err := http.NewRequest(http.MethodGet, packUri, nil)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	getRequest.Header = r.Header.Clone()
	getRequest.Header.Set("Range", fmt.Sprintf("bytes=%s", itemRange))

	getResponse, err := p.client.Do(getRequest)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if getResponse.StatusCode == http.StatusPartialContent {
		getResponse.StatusCode = http.StatusOK
		getResponse.Status = http.StatusText(http.StatusOK)
		getResponse.Header.Del("Content-Range")
		getResponse.Header.Del("X-Object-Meta-Pack")
	}

	if err := writeResponse(w, getResponse); err != nil {
		debugf("Cannot write response: " + err.Error())
	}
}

func (p *packer) createOrGetPack(groupId string, first *http.Request) (*pack, error) {
	apack, ok := p.packs[groupId]
	if ok {
		return apack, nil
	}

	account, container, _, err := parseRequestParts(first.URL.Path)
	if err != nil {
		return nil, err
	}

	apack = &pack{
		config:  p.config,
		client:  p.client,

		groupId: groupId,
		packId:  randomHexChars(32),

		account:   account,
		container: container,

		first: first,
		parts: make([]*part, 0),
		size:  0,

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

		go apack.packWorker()
	}()

	p.packs[groupId] = apack
	return apack, nil
}

func (p *packer) deletePack(groupId string) {
	delete(p.packs, groupId)
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

func (p *packer) parseGroupId(request *http.Request) (string, error) {
	if request.Header.Get("X-Pack-Group") != "" {
		return request.Header.Get("X-Pack-Group"), nil
	} else {
		account, container, _, err := parseRequestParts(request.URL.Path)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s/%s", account, container), nil
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

func copyWithDefaults(config *Config) (*Config, error) {
	// Validate mandatory fields
	if config.ForwardAddr == "" {
		return nil, errors.New("forwarding address must be set")
	}

	// Create prepopulated config and override fields
	newConfig := &Config{
		ForwardAddr: config.ForwardAddr,
		ListenAddr:  ":1234",
		MinSize:     128 * 1024,
		MaxWait:     100 * time.Millisecond,
		MaxCount:    50,
		Prefix:      ":pack",
	}

	if config.ListenAddr != "" {
		newConfig.ListenAddr = config.ListenAddr
	}

	if config.MinSize != 0 {
		newConfig.MinSize = config.MinSize
	}

	if config.MaxWait != 0 {
		newConfig.MaxWait = config.MaxWait
	}

	if config.MaxCount != 0 {
		newConfig.MaxCount = config.MaxCount
	}

	if config.Prefix != "" {
		newConfig.Prefix = config.Prefix
	}

	return newConfig, nil
}

func randomHexChars(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = hexCharset[rand.Intn(len(hexCharset))]
	}
	return string(b)
}

func parseRequestParts(path string) (string, string, string, error) {
	matches := accountContainerObjectRegex.FindStringSubmatch(path)
	if matches == nil || len(matches) != 4 {
		return "", "", "", errors.New("unable to parse path into account/container/object")
	}

	return matches[1], matches[2], matches[3], nil
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
