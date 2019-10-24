package packer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPacker(t *testing.T) {
	var counter int32

	// Start fake Swift server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Cannot read body for request %s: %s\n", r.URL.Path, err.Error())
			t.FailNow()
		}

		t.Logf("Fake Swift received request %s with payload %s\n", r.URL.Path, string(body))
		w.WriteHeader(201)
		atomic.AddInt32(&counter, 1)
	})

	go http.ListenAndServe("127.0.0.1:22586", nil)

	// Start packer
	packer := NewPacker(&Config{
		ListenAddr:  "127.0.0.1:12586",
		ForwardAddr: "127.0.0.1:22586",
		MinSize:     50,
		MaxWait:     200 * time.Millisecond,
	})

	go packer.ListenAndServe()

	// Generate some requests
	var wg sync.WaitGroup
	client := &http.Client{}

	wg.Add(101)
	go func() {
		for i := 0; i < 101; i++ {
			// time.Sleep(20 * time.Millisecond)

			go func(v int) {
				payload := fmt.Sprintf("BLA%03d", v)
				uri := fmt.Sprintf("http://127.0.0.1:12586/v1/AUTH_admin/mycontainer/myobject%d", v)
				request, err := http.NewRequest("PUT", uri,	strings.NewReader(payload))
				if err != nil {
					t.Error(err)
					t.FailNow()
				}

				request.Header.Set("X-Pack", "yes")
				request.Header.Set("X-Pack-Group", "default")

				t.Logf("PUT %s (with payload %s)\n", request.URL.Path, payload)
				response, err := client.Do(request)
				if err != nil {
					t.Error(err)
					t.FailNow()
				}

				if response.StatusCode != 201 {
					t.Errorf("Request %s did not return 201 status code: %s\n", request.URL.Path, response.Status)
					t.FailNow()
				}

				if response.Header.Get("X-Pack-Id") == "" ||
					response.Header.Get("X-Item-Offset") == "" ||
					response.Header.Get("X-Item-Length") == "" {

					t.Errorf("Expected headers not in response")
					t.FailNow()
				}

				wg.Done()
			}(i)
		}
	}()

	// t.Fail()

	wg.Wait()

	// Each payload is 6 bytes, min pack size is 50 -> ceil(50/6) = 9 PUTs per pack
	// For 101 PUTs, that's ceil(101/9) = 12 packs
	if counter != 12 {
		t.Errorf("Fake swift backend received %d PUTs, expected %d", counter, 12)
		t.FailNow()
	}
}