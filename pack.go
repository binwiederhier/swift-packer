package packer

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type pack struct {
	config  *Config
	client *http.Client

	groupId string
	packId  string

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
		response = &http.Response{
			StatusCode: 500,
			Status: "500 Internal Server Error",
		}
	}

	p.sendResponses(response)
}

func (p *pack) packUpload() (*http.Response, error) {
	prefix, err := parsePrefix(p.first.URL.Path)
	if err != nil {
		return nil, err
	}

	uri := fmt.Sprintf("http://%s/%s/%s", p.config.ForwardAddr, prefix, p.packId)
	debugf("Uploading pack %s (group %s) to %s\n", p.packId, p.groupId, uri)

	readers := make([]io.Reader, 0)
	for _, apart := range p.parts {
		readers = append(readers, bytes.NewReader(apart.data))
	}

	upstreamRequest, err := http.NewRequest("PUT", uri, io.MultiReader(readers...))
	if err != nil {
		return nil, err
	}

	upstreamRequest.Header = p.first.Header.Clone()
	upstreamResponse, err := p.client.Do(upstreamRequest)
	if err != nil {
		return nil, err
	}

	debugf("Finished uploading pack %s (group %s), len %d, count %d, status %s\n",
		p.packId, p.groupId, p.size, len(p.parts), upstreamResponse.Status)

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
	for _, apart := range p.parts {
		partResponse := &http.Response{
			Status:     response.Status,
			StatusCode: response.StatusCode,
			Header:     response.Header.Clone(),
		}

		partResponse.Header.Add("X-Pack-Id", p.packId)
		partResponse.Header.Add("X-Item-Offset", fmt.Sprintf("%d", apart.offset))
		partResponse.Header.Add("X-Item-Length", fmt.Sprintf("%d", len(apart.data)))

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
	return p.size > p.config.MinSize
}
