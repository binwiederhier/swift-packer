package packer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestPacker(t *testing.T) {
	// Start fake Swift server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Cannot read body for request %s: %s\n", r.URL.Path, err.Error())
			t.FailNow()
		}

		t.Logf("Fake Swift received request %s with payload %s\n", r.URL.Path, string(body))
		w.WriteHeader(201)
	})

	go http.ListenAndServe("127.0.0.1:22586", nil)

	// Start packer
	packer := NewPacker(&Config{
		ListenAddr:  "127.0.0.1:12586",
		ForwardAddr: "127.0.0.1:22586",
		MinSize:     5,
		MaxWait:     200,
	})

	go packer.ListenAndServe()

	// Generate some requests
	var wg sync.WaitGroup
	client := &http.Client{}

	wg.Add(100)
	go func() {
		for i := 0; i < 100; i++ {
			// time.Sleep(20 * time.Millisecond)

			go func(v int) {
				payload := fmt.Sprintf("PUT%d", v)
				request, err := http.NewRequest("PUT", "http://127.0.0.1:12586/v1/AUTH_admin/mycontainer/myobject",
					strings.NewReader(payload))
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

				wg.Done()
			}(i)
		}
	}()

	// t.Fail()

	wg.Wait()
}