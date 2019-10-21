package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"sync"
)

type xpack struct {
	id int
	full bool
	requests chan *string
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

	p.length += len(*request)
	p.count++

	if p.length > 40 {
		p.more <-false
	} else {
		p.more <-true
	}

	return len(*request), nil
}

func newPack(id int) *xpack {
	return &xpack{
		id: id,
		full: false,
		requests: make(chan *string),
		more: make(chan bool),
	}
}

func main() {
	requests := make(map[string]chan *string)
	var mu sync.Mutex

	go func() {
		for i := 0; i < 1000000; i++ {
			//time.Sleep(20 * time.Millisecond)

			// ServeHTTP
			go func(v int) {
				put := fmt.Sprintf("PUT%d", v)
				group := "default" // fmt.Sprintf("%d", rand.Intn(5))

				mu.Lock()
				ch, ok := requests[group]
				if !ok {
					ch = make(chan *string)
					requests[group] = ch
					go dispatch(ch)
				}
				mu.Unlock()
				log.Printf("ServeHTTP: received %s-%s\n", group, put)

				ch <- &put
				log.Printf("ServeHTTP: dispatched %s-%s\n", group, put)
			}(i)

			runtime.Gosched()
		}
	}()


	select{}
}

func dispatch(ch chan *string) {
	log.Printf("Dispatch: started pack dispatch\n")
	packId := 0
	client := &http.Client{}
	var pack *xpack
	var more bool
	for request := range ch {
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

				response, err := client.Do(request)
				if err != nil {
					panic(err)
				}

				log.Printf("handlePack: pack %d uploaded, len %d, count %d, status %s\n", p.id, p.length, p.count, response.Status)
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
