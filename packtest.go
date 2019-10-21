package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
)

type xpack struct {
	id int
	full bool
	requests chan *http.Request
	more chan bool
	length int
	count int
}

func (p *xpack) Read(b []byte) (int, error) {
	request := <- p.requests
	if request == nil {
		log.Printf("Read: pack %d -> nil request received. Closing pack\n", p.id)
		return 0, io.EOF
	}

	log.Printf("Read: pack %d -> appending %s\n", p.id, *request)

	l := rand.Intn(20)
	p.length += l
	p.count++

	if p.length > 40 {
		p.more <-false
	} else {
		p.more <-true
	}

	return l, nil
}

func newPack(id int) *xpack {
	return &xpack{
		id: id,
		full: false,
		requests: make(chan *http.Request),
		more: make(chan bool),
	}
}

type packGroup struct {
	client *http.Client
	requests  chan *http.Request
	responses chan *http.Response
}

func newPackGroup(client *http.Client) *packGroup {
	return &packGroup{
		client:    client,
		requests:  make(chan *http.Request),
		responses: make(chan *http.Response),
	}
}

func main() {
	packer := newPacker()

	go func() {
		for i := 0; i < 1000; i++ {
			//time.Sleep(20 * time.Millisecond)

			// ServeHTTP
			go func(v int) {
				put := fmt.Sprintf("PUT%d", v)
				group := "default" // fmt.Sprintf("%d", rand.Intn(5))

				g := packer.group(group)
				log.Printf("ServeHTTP: received %s-%s\n", group, put)

				g.requests <- &http.Request{Body: ioutil.NopCloser(strings.NewReader(put))}
				log.Printf("ServeHTTP: dispatched %s-%s\n", group, put)
				response := <- g.responses
				log.Printf("ServeHTTP: response received for %s: %s\n", put, response.Status)
			}(i)

			runtime.Gosched()
		}
	}()


	select{}
}

type packer struct {
	client *http.Client
	groups map[string]*packGroup
	sync.Mutex
}

func newPacker() *packer {
	return &packer{
		client: &http.Client{},
		groups: make(map[string]*packGroup, 0),
	}
}

func (p *packer) group(groupId string) *packGroup {
	p.Lock()
	defer p.Unlock()

	group, ok := p.groups[groupId]
	if !ok {
		group = newPackGroup(p.client)
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

func (g *packGroup) requestHandler() {
	log.Printf("Dispatch: started pack dispatch\n")
	packId := 0
	var pack *xpack
	var more bool
	var request *http.Request

	for {
		select {
		case request = <-g.requests:
			break
		case <-time.After(2000 * time.Millisecond):
			if pack != nil {
				log.Printf("Dispatch: pack %d timed out\n", pack.id)
				pack.requests <- nil
				return
			}
		}

		if pack != nil && !more {
			log.Printf("Dispatch: pack %d is full\n", pack.id)
			pack.requests <- nil
			pack = nil
			packId++
		}

		if pack == nil {
			pack = newPack(packId)

			// handlePack
			go func(p *xpack) {
				uri := fmt.Sprintf("http://127.0.0.1:2586/v1/AUTH_hammer%s/%s/packtest/%d", randomHexChars2(2), randomHexChars2(3), p.id)
				log.Printf("handlePack: pack %d -> uploading to %s\n", pack.id, uri)

				request, err := http.NewRequest("PUT", uri, p)
				if err != nil {
					panic(err)
				}
				request.Header.Set("X-Auth-Token", "3dd56b6df50b64af360af879fbfcea09")

				response, err := g.client.Do(request)
				if err != nil {
					panic(err)
				}

				log.Printf("handlePack: pack %d uploaded, len %d, count %d, status %s\n", p.id, p.length, p.count, response.Status)

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
				for i := 0; i < p.count; i++ {
					downResponse := response
					if body != nil {
						downResponse.Body = ioutil.NopCloser(bytes.NewReader(body))
					}
					g.responses <- downResponse
				}
			}(pack)
		}

		pack.requests <- request
		more = <- pack.more
	}
}

var charset2 = []rune("0123456789abcdef")

func randomHexChars2(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = charset2[rand.Intn(len(charset2))]
	}
	return string(b)
}
