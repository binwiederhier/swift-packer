package packer

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type pack struct {
	config  *Config
	client *http.Client

	groupId string
	packId  string

	account   string
	container string

	first *http.Request
	parts []*part
	size  int

	timer *time.Timer
	done  chan bool
}

type part struct {
	response chan<- *http.Response
	offset   int
	data     []byte
}

func (p *pack) packWorker() {
	response, err := p.packUpload()
	if err != nil {
		debugf(err.Error())
		response = &http.Response{
			StatusCode: 500,
			Status: "500 Internal Server Error",
			Header: http.Header{},
		}
	}

	p.sendResponses(response)
}

func (p *pack) packUpload() (*http.Response, error) {
	uri := fmt.Sprintf("http://%s/v1/%s/%s/%s/%s", p.config.ForwardAddr, p.account, p.container, p.config.Prefix, p.packId)

	readers := make([]io.Reader, 0)
	for _, apart := range p.parts {
		readers = append(readers, bytes.NewReader(apart.data))
	}

	upstreamRequest, err := http.NewRequest(http.MethodPut, uri, io.MultiReader(readers...))
	if err != nil {
		return nil, err
	}

	meta := make([]string, 0)
	for _, apart := range p.parts {
		meta = append(meta, fmt.Sprintf("%d-%d", apart.offset, apart.offset + len(apart.data) - 1))
	}

	upstreamRequest.Header = p.first.Header.Clone()
	upstreamRequest.Header.Set("X-Object-Meta-Pack", strings.Join(meta, ","))

	upstreamResponse, err := p.client.Do(upstreamRequest)
	if err != nil {
		return nil, err
	}

	logf("PUT /v1/%s/%s/%s/%s type=initial len=%d count=%d status=%s\n",
		p.account, p.container, p.config.Prefix, p.packId, p.size, len(p.parts), upstreamResponse.Status)

	return upstreamResponse, nil
}

func (p *pack) sendResponses(response *http.Response) {
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
	packPath := fmt.Sprintf("%s/%s/%s/%s", p.account, p.container, p.config.Prefix, p.packId)

	for i, apart := range p.parts {
		partResponse := &http.Response{
			Status:     response.Status,
			StatusCode: response.StatusCode,
			Header:     response.Header.Clone(),
		}

		partResponse.Header.Add("X-Pack-Path", packPath)
		partResponse.Header.Add("X-Item-Path", fmt.Sprintf("%s/%d", packPath, i))

		if body != nil {
			partResponse.Body = ioutil.NopCloser(bytes.NewReader(body))
		}

		apart.response <- partResponse
	}
}

func (p *pack) append(data []byte, responseChan chan<- *http.Response) {
	offset := p.size
	p.size += len(data)
	p.parts = append(p.parts, &part{
		offset: offset,
		data: data,
		response: responseChan,
	})
	p.timer.Reset(p.config.MaxWait)
}

func (p *pack) full() bool {
	return p.size > p.config.MinSize || len(p.parts) >= p.config.MaxCount
}
