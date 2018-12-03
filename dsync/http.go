package dsync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/qri-io/dag"
)

// HTTPRemote implents the Remote interface via HTTP POST requests
type HTTPRemote struct {
	URL string
}

// ReqSession initiates a send session
func (rem *HTTPRemote) ReqSession(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error) {
	buf := &bytes.Buffer{}
	if err = json.NewEncoder(buf).Encode(mfst); err != nil {
		return
	}

	req, err := http.NewRequest("POST", rem.URL, buf)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}

	if res.StatusCode != http.StatusOK {
		var msg string
		if data, err := ioutil.ReadAll(res.Body); err == nil {
			msg = string(data)
		}
		err = fmt.Errorf("remote repsonse: %d %s", res.StatusCode, msg)
		return
	}

	sid = res.Header.Get("sid")
	diff = &dag.Manifest{}
	err = json.NewDecoder(res.Body).Decode(diff)
	fmt.Printf("sid: %s. sending %d blocks\n", sid, len(diff.Nodes))
	return
}

// PutBlock sends a block over HTTP to a remote source
func (rem *HTTPRemote) PutBlock(sid, hash string, data []byte) Response {
	url := fmt.Sprintf("%s?sid=%s&hash=%s", rem.URL, sid, hash)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(data))
	if err != nil {
		return Response{
			Hash:   hash,
			Status: StatusErrored,
			Err:    err,
		}
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return Response{
			Hash:   hash,
			Status: StatusErrored,
			Err:    err,
		}
	}

	if res.StatusCode != http.StatusOK {
		var msg string
		if data, err := ioutil.ReadAll(res.Body); err == nil {
			msg = string(data)
		}
		return Response{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("remote error: %d %s", res.StatusCode, msg),
		}
	}

	return Response{
		Hash:   hash,
		Status: StatusOk,
	}
}
