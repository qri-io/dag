package dsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/qri-io/dag"
)

// HTTPClient implents the Remote interface via HTTP requests.
type HTTPClient struct {
	URL string
}

// PushStart initiates a send session. It sends a Manifest to a remote source over HTTP
func (rem *HTTPClient) PushStart(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error) {
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
		err = fmt.Errorf("remote response: %d %s", res.StatusCode, msg)
		return
	}

	sid = res.Header.Get("sid")
	diff = &dag.Manifest{}
	err = json.NewDecoder(res.Body).Decode(diff)
	// TODO (b5): this is really terrible to print here, but is *very* helpful info on the CLI
	// we should pipe a completion channel up to the CLI & remove this
	fmt.Printf("   sending %d/%d blocks (session id: %s)\n", len(diff.Nodes), len(mfst.Nodes), sid)
	return
}

// PushBlock sends a block over HTTP to a remote source
func (rem *HTTPClient) PushBlock(sid, hash string, data []byte) Response {
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

// PullManifest gets a manifest from a remote source over HTTP
func (rem *HTTPClient) PullManifest(ctx context.Context, id string) (mfst *dag.Manifest, err error) {
	url := fmt.Sprintf("%s?manifest=%s", rem.URL, id)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		var msg string
		if data, err := ioutil.ReadAll(res.Body); err == nil {
			msg = string(data)
		}
		return nil, fmt.Errorf("remote error: %d %s", res.StatusCode, msg)
	}
	defer res.Body.Close()

	mfst = &dag.Manifest{}
	err = json.NewDecoder(res.Body).Decode(mfst)
	return
}

// PullBlock fetches a block from a remote source over HTTP
func (rem *HTTPClient) PullBlock(ctx context.Context, id string) (data []byte, err error) {
	url := fmt.Sprintf("%s?block=%s", rem.URL, id)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		var msg string
		if data, err := ioutil.ReadAll(res.Body); err == nil {
			msg = string(data)
		}
		return nil, fmt.Errorf("remote error: %d %s", res.StatusCode, msg)
	}
	defer res.Body.Close()

	return ioutil.ReadAll(res.Body)
}

// HTTPTransfersHandler exposes Transfers over HTTP, interlocks with methods exposed by HTTPClient
func HTTPTransfersHandler(rs *Transfers) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			mfst := &dag.Manifest{}
			if err := json.NewDecoder(r.Body).Decode(mfst); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
			r.Body.Close()

			sid, diff, err := rs.PushStart(mfst)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Set("sid", sid)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(diff)

		case "PUT":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			res := rs.PushBlock(r.FormValue("sid"), r.FormValue("hash"), data)

			if res.Status == StatusErrored {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(res.Err.Error()))
			} else if res.Status == StatusRetry {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(res.Err.Error()))
			} else {
				w.WriteHeader(http.StatusOK)
			}
		case "GET":
			mfstID := r.FormValue("manifest")
			blockID := r.FormValue("block")
			if mfstID == "" && blockID == "" {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("either manifest or block query params are required"))
			} else if mfstID != "" {
				mfst, err := rs.PullManifest(r.Context(), mfstID)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
					return
				}

				data, err := json.Marshal(mfst)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.Write(data)
			} else {
				data, err := rs.PullBlock(r.Context(), blockID)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Write(data)
			}
		}
	}
}
