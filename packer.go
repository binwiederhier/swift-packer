package packer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"mime"
	"mime/multipart"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Packer interface {
	ListenAndServe() error
}

type Config struct {
	ForwardAddr     string
	ListenAddr      string
	MinSize         int
	MaxWait         time.Duration
	MaxCount        int
	RepackThreshold int
	Prefix          string
}

type packer struct {
	config  *Config
	client  *http.Client
	buffers *sync.Pool
	packs   map[string]*pack
	sync.Mutex
}

var (
	Debug                       = false // TODO Proper log levels

	hexCharset                  = []rune("0123456789abcdef")
	accountContainerObjectRegex = regexp.MustCompile(`^/v1/([^/]+)/([^/]+)/(.+)$`)
	packItemRegex               = regexp.MustCompile(`^([^/]+)/([^/]+)/(\d+)$`)
	itemRangeRegex              = regexp.MustCompile(`^(\d+)-(\d+)$`)
)

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func NewPacker(config *Config) (Packer, error) {
	newConfig, err := copyWithDefaults(config)
	if err != nil {
		return nil, err
	}

	return &packer{
		config:  newConfig,
		client:  &http.Client{},
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
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if status, err := p.serveHTTP(w, r); err != nil {
				http.Error(w, err.Error(), status)
			}
		}),
	}

	logf("Listening on %s\n", p.config.ListenAddr)
	return server.ListenAndServe()
}

func (p *packer) serveHTTP(w http.ResponseWriter, r *http.Request) (int, error) {
	if r.Method == http.MethodPut && r.Header.Get("X-Allow-Pack") == "yes" {
		return p.handlePUT(w, r)
	} else if r.Method == http.MethodGet {
		return p.handleGET(w, r)
	} else if r.Method == http.MethodDelete {
		return p.handleDELETE(w, r)
	}

	return p.forwardRequest(w, r)
}

func (p *packer) handlePUT(w http.ResponseWriter, request *http.Request) (int, error) {
	// Read up to buffer size into memory
	buffer := p.buffers.Get().([]byte)
	defer p.buffers.Put(buffer)

	read, err := p.readBuffer(buffer, request.Body)
	if err != nil {
		request.Body.Close() // Ignore error!
		return 500, err
	}

	// Request is at least as large as buffer, forward upstream
	if read == len(buffer) {
		debugf("Incoming PUT %s too large for packing (> %d bytes), forwarding upstream\n", request.URL.Path, read)
		request.Body = ioutil.NopCloser(io.MultiReader(bytes.NewReader(buffer[:read]), request.Body)) // FIXME leaking body
		return p.forwardRequest(w, request)
	}

	// Request is fully in the buffer now, close request body and start packing
	if err := request.Body.Close(); err != nil {
		return 500, err
	}

	debugf("Incoming PUT %s fully read (%d bytes), appending to pack\n", request.URL.Path, read)

	// Determine pack group
	groupId, err := p.parseGroupId(request)
	if err != nil {
		return 400, err
	}

	// Find pack, append part to it
	p.Lock()
	apack, err := p.createOrGetPack(groupId, request)
	if err != nil {
		return 400, err
	}

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

	debugf("Dispatching response for %s, packed in %s (group %s), response: %s\n",
		apack.packId, apack.groupId, request.URL.Path, response.Status)

	if err := writeResponse(w, response); err != nil {
		debugf("Cannot write response: " + err.Error())
	}

	return 0, nil
}

func (p *packer) handleGET(w http.ResponseWriter, r *http.Request) (int, error) {
	account, container, object, err := parseRequestParts(r.URL.Path)
	if err != nil {
		return 400, err
	}

	packItemParts := packItemRegex.FindStringSubmatch(object)
	if len(packItemParts) != 4 || packItemParts[1] != p.config.Prefix {
		return p.forwardRequest(w, r)
	}

	packId := packItemParts[2]
	itemId, err := strconv.Atoi(packItemParts[3])
	if err != nil {
		return 400, err
	}

	packUri := fmt.Sprintf("http://%s/v1/%s/%s/%s/%s", p.config.ForwardAddr, account, container, p.config.Prefix, packId)
	headRequest, err := http.NewRequest(http.MethodHead, packUri, nil)
	if err != nil {
		return 500, err
	}

	headRequest.Header = r.Header.Clone()
	headResponse, err := p.client.Do(headRequest)
	if err != nil {
		return 500, err
	}

	meta := headResponse.Header.Get("X-Object-Meta-Pack")
	if meta == "" {
		return 500, errors.New("Pack file does not contain X-Object-Meta-Pack header")
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
		return 404, errors.New("Requested item does not exist in pack")
	}

	getRequest, err := http.NewRequest(http.MethodGet, packUri, nil)
	if err != nil {
		return 500, err
	}

	getRequest.Header = r.Header.Clone()
	getRequest.Header.Set("Range", fmt.Sprintf("bytes=%s", itemRange))

	getResponse, err := p.client.Do(getRequest)
	if err != nil {
		return 500, err
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

	logf("GET /v1/%s/%s/%s/%s type=range item=%s status=%s\n",
		account, container, p.config.Prefix, packId, getResponse.Status)

	return 0, nil
}

func (p *packer) handleDELETE(w http.ResponseWriter, r *http.Request) (int, error) {
	account, container, object, err := parseRequestParts(r.URL.Path)
	if err != nil {
		return 400, err
	}

	packItemParts := packItemRegex.FindStringSubmatch(object)
	if len(packItemParts) != 4 || packItemParts[1] != p.config.Prefix {
		return p.forwardRequest(w, r)
	}

	packId := packItemParts[2]
	itemId, err := strconv.Atoi(packItemParts[3])
	if err != nil {
		return 400, err
	}

	packPath := fmt.Sprintf("%s/%s/%s/%s", account, container, p.config.Prefix, packId)
	packUri := fmt.Sprintf("http://%s/v1/%s", p.config.ForwardAddr, packPath)
	headRequest, err := http.NewRequest(http.MethodHead, packUri, nil)
	if err != nil {
		return 500, err
	}

	headRequest.Header = r.Header.Clone()
	headResponse, err := p.client.Do(headRequest)
	if err != nil {
		return 500, err
	}

	packSize, err := strconv.Atoi(headResponse.Header.Get("Content-Length"))
	if err != nil {
		return 500, err
	}

	meta := headResponse.Header.Get("X-Object-Meta-Pack")
	if meta == "" {
		return 500, errors.New("Pack file does not contain X-Object-Meta-Pack header")
	}

	// Logically delete item
	packMetaParts := strings.Split(meta, ",")
	if itemId < 0 || itemId > len(packMetaParts)-1 {
		return 400, errors.New("Invalid item ID")
	}
	packMetaParts[itemId] = ""

	// Find out if X% of pack have been logically deleted, repack if it has
	logicalPackSize := 0
	for _, itemRange := range packMetaParts {
		rangeParts := itemRangeRegex.FindStringSubmatch(itemRange)
		if rangeParts != nil {
			from, err := strconv.Atoi(rangeParts[1])
			if err != nil {
				return 500, err
			}
			to, err := strconv.Atoi(rangeParts[2])
			if err != nil {
				return 500, err
			}
			logicalPackSize += to - from + 1
		}
	}

	if float32(logicalPackSize) > float32(packSize) * (1 - float32(p.config.RepackThreshold)/100) {
		// Update X-Object-Meta-Pack (remove item range)
		postRequest, err := http.NewRequest(http.MethodPost, packUri, nil)
		if err != nil {
			return 500, err
		}

		postRequest.Header = r.Header.Clone()
		postRequest.Header.Set("X-Object-Meta-Pack", strings.Join(packMetaParts, ","))

		postResponse, err := p.client.Do(postRequest)
		if err != nil {
			return 500, err
		}

		if err := writeResponse(w, postResponse); err != nil {
			debugf("Cannot write response: " + err.Error())
		}

		logf("DELETE /v1/%s/%s/%s/%s type=logical item=%d status=%s\n",
			account, container, p.config.Prefix, packId, itemId, postResponse.Status)
	} else {
		// Get all bytes for non-empty ranges from backend
		getRequestRangeBytes := make([]string, 0)
		for _, itemRange := range packMetaParts {
			if itemRange != "" {
				getRequestRangeBytes = append(getRequestRangeBytes, itemRange)
			}
		}

		getRequest, err := http.NewRequest(http.MethodGet, packUri, nil)
		if err != nil {
			return 500, err
		}

		getRequest.Header = r.Header.Clone()
		getRequest.Header.Set("Range", "bytes=" + strings.Join(getRequestRangeBytes,","))

		getResponse, err := p.client.Do(getRequest)
		if err != nil {
			return 500, err
		}

		mediaType, params, err := mime.ParseMediaType(getResponse.Header.Get("Content-Type"))
		if err != nil {
			return 500, err
		}

		off := 0
		body := make([]byte, logicalPackSize)
		if strings.HasPrefix(mediaType, "multipart/") {
			mr := multipart.NewReader(getResponse.Body, params["boundary"])
			for off < len(body) {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				} else if err != nil {
					return 500, err
				}

				for off < len(body) {
					read, err := p.Read(body[off:])
					if err == io.EOF {
						off += read
						break
					} else if err != nil {
						return 500, err
					}
					off += read
				}
			}
		} else {
			body, err = ioutil.ReadAll(getResponse.Body)
			if err != nil {
				return 500, err
			}
		}

		if err := getResponse.Body.Close(); err != nil {
			return 500, err
		}

		// Update pack meta data, and put the re-packed object back
		// Examples:
		//   X-Object-Meta-Pack: 0-131,,,393-524 -> 0-131,,,131-262
		//   X-Object-Meta-Pack:,,10-20,,40-51 -> ,,0-10,,10-21
		newPackMetaParts := make([]string, 0)
		lastTo := 0
		for _, itemRange := range packMetaParts {
			rangeParts := itemRangeRegex.FindStringSubmatch(itemRange)
			if rangeParts != nil {
				from, err := strconv.Atoi(rangeParts[1])
				if err != nil {
					return 500, err
				}
				to, err := strconv.Atoi(rangeParts[2])
				if err != nil {
					return 500, err
				}
				length := to - from
				if lastTo == 0 {
					from = 0
					to  = length
				} else {
					from = lastTo + 1
					to = from + length
				}
				newPackMetaParts = append(newPackMetaParts, fmt.Sprintf("%d-%d", from, to))
				lastTo = to
			} else {
				newPackMetaParts = append(newPackMetaParts, "")
			}
		}

		putRequest, err := http.NewRequest(http.MethodPut, packUri, bytes.NewReader(body))
		if err != nil {
			return 500, err
		}

		putRequest.Header = r.Header.Clone()
		putRequest.Header.Set("X-Object-Meta-Pack", strings.Join(newPackMetaParts, ","))

		debugf("PUT /v1/%s (group %s) \n", packPath, strings.Join(newPackMetaParts, ","))
		putResponse, err := p.client.Do(putRequest)
		if err != nil {
			return 500, err
		}

		if err := writeResponse(w, putResponse); err != nil {
			debugf("Cannot write response: " + err.Error())
		}

		logf("DELETE /v1/%s/%s/%s/%s type=repack item=%d status=%s\n",
			account, container, p.config.Prefix, packId, itemId, putResponse.Status)
	}

	return 0, nil
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

func (p *packer) forwardRequest(w http.ResponseWriter, request *http.Request) (int, error) {
	forwardURL := request.URL
	forwardURL.Scheme = "http"
	forwardURL.Host = p.config.ForwardAddr

	proxyRequest, err := http.NewRequest(request.Method, forwardURL.String(), request.Body)
	if err != nil {
		return 500, err
	}

	proxyRequest.Header = request.Header.Clone()
	proxyRequest.Header.Set("Host", request.Host)

	proxyResponse, err := p.client.Do(proxyRequest)
	if err != nil {
		return 500, err
	}

	if err := writeResponse(w, proxyResponse); err != nil {
		debugf("Cannot write response to forwarded request: " + err.Error())
	}

	return 0, nil
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
		ForwardAddr:     config.ForwardAddr,
		ListenAddr:      ":1234",
		MinSize:         128 * 1024,
		MaxWait:         100 * time.Millisecond,
		MaxCount:        10,
		RepackThreshold: 20,
		Prefix:          ":pack",
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

	if config.RepackThreshold != 0 {
		newConfig.RepackThreshold = config.RepackThreshold
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

func debugf(format string, args ...interface{}) {
	if Debug {
		log.Printf(format, args...)
	}
}

func logf(format string, args ...interface{}) {
	log.Printf(format, args...)
}