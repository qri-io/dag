package dsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/qri-io/dag"
)

const (
	httpProtcolIDHeader = "dsync-version"
)

// HTTPClient is the request side of doing dsync over HTTP
type HTTPClient struct {
	URL           string
	remProtocolID protocol.ID
}

var (
	// HTTPClient exists to satisfy the DaySyncable interface on the client side
	// of a transfer
	_ DagSyncable   = (*HTTPClient)(nil)
	_ DagStreamable = (*HTTPClient)(nil)
)

// NewReceiveSession initiates a session for pushing blocks to a remote.
// It sends a Manifest to a remote source over HTTP
func (rem *HTTPClient) NewReceiveSession(info *dag.Info, pinOnComplete bool, meta map[string]string) (sid string, diff *dag.Manifest, err error) {
	buf := &bytes.Buffer{}
	if err = json.NewEncoder(buf).Encode(info); err != nil {
		return
	}

	u, err := url.Parse(rem.URL)
	if err != nil {
		return
	}
	q := u.Query()
	q.Set("pin", fmt.Sprintf("%t", pinOnComplete))
	for key, val := range meta {
		q.Set(key, val)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("POST", u.String(), buf)
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

	protocolIDHeaderStr := res.Header.Get(httpProtcolIDHeader)
	if protocolIDHeaderStr == "" {
		// protocol ID header only exists in version 0.2.0 and up, when header isn't
		// present assume version 0.1.1, the latest version before header was set
		// 0.1.1 is wire-compatible with all lower versions of dsync
		rem.remProtocolID = protocol.ID("/dsync/0.1.1")
	} else {
		rem.remProtocolID = protocol.ID(protocolIDHeaderStr)
	}

	diff = &dag.Manifest{}
	err = json.NewDecoder(res.Body).Decode(diff)

	return
}

// ProtocolVersion indicates the version of dsync the remote speaks, only
// available after a handshake is established
func (rem *HTTPClient) ProtocolVersion() (protocol.ID, error) {
	if string(rem.remProtocolID) == "" {
		return "", ErrUnknownProtocolVersion
	}
	return rem.remProtocolID, nil
}

// PutBlocks streams a manifest of blocks to the remote in one HTTP call
func (rem *HTTPClient) PutBlocks(ctx context.Context, sid string, ng ipld.NodeGetter, mfst *dag.Manifest, progCh chan cid.Cid) error {
	r, err := NewManifestCARReader(ctx, ng, mfst, progCh)
	if err != nil {
		log.Debugf("err creating CARReader err=%q ", err)
		return err
	}

	req, err := http.NewRequest("PATCH", fmt.Sprintf("%s?sid=%s", rem.URL, sid), r)
	if err != nil {
		log.Debugf("err creating PATCH HTTP request err=%q ", err)
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Debugf("err doing HTTP request. err=%q", err)
		return err
	}

	if res.StatusCode != http.StatusOK {
		var msg string
		if data, err := ioutil.ReadAll(res.Body); err == nil {
			msg = string(data)
		}
		log.Debugf("error response from remote. err=%q", msg)
		return fmt.Errorf("remote response: %d %s", res.StatusCode, msg)
	}

	return nil
}

// FetchBlocks streams a manifest of requested blocks
func (rem *HTTPClient) FetchBlocks(ctx context.Context, sid string, mfst *dag.Manifest, progCh chan cid.Cid) error {
	return fmt.Errorf("not implemented")
}

// ReceiveBlock asks a remote to receive a block over HTTP
func (rem *HTTPClient) ReceiveBlock(sid, hash string, data []byte) ReceiveResponse {
	url := fmt.Sprintf("%s?sid=%s&hash=%s", rem.URL, sid, hash)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(data))
	if err != nil {
		log.Debugf("http client create request error=%s", err)
		return ReceiveResponse{
			Hash:   hash,
			Status: StatusErrored,
			Err:    err,
		}
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Debugf("http client perform request error=%s", err)
		return ReceiveResponse{
			Hash:   hash,
			Status: StatusRetry,
			Err:    fmt.Errorf("performing HTTP PUT: %w", err),
		}
	}

	if res.StatusCode != http.StatusOK {
		var msg string
		if data, err := ioutil.ReadAll(res.Body); err == nil {
			msg = string(data)
		}
		return ReceiveResponse{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("remote error: %d %s", res.StatusCode, msg),
		}
	}

	return ReceiveResponse{
		Hash:   hash,
		Status: StatusOk,
	}
}

// GetDagInfo fetches a manifest from a remote source over HTTP
func (rem *HTTPClient) GetDagInfo(ctx context.Context, id string, meta map[string]string) (info *dag.Info, err error) {
	u, err := url.Parse(rem.URL)
	if err != nil {
		return
	}
	q := u.Query()
	q.Set("manifest", id)
	for key, val := range meta {
		q.Set(key, val)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
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

	info = &dag.Info{}
	err = json.NewDecoder(res.Body).Decode(info)
	return
}

// GetBlock fetches a block from a remote source over HTTP
func (rem *HTTPClient) GetBlock(ctx context.Context, id string) (data []byte, err error) {
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

// RemoveCID asks a remote to remove a CID
func (rem *HTTPClient) RemoveCID(ctx context.Context, id string, meta map[string]string) (err error) {
	u, err := url.Parse(rem.URL)
	if err != nil {
		return
	}
	q := u.Query()
	q.Set("cid", id)
	for key, val := range meta {
		q.Set(key, val)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		var msg string
		if data, err := ioutil.ReadAll(res.Body); err == nil {
			msg = string(data)
		}
		if msg == ErrRemoveNotSupported.Error() {
			return ErrRemoveNotSupported
		}
		return fmt.Errorf("remote: %d %s", res.StatusCode, msg)
	}

	return nil
}

// HTTPRemoteHandler exposes a Dsync remote over HTTP by exposing a HTTP handler
// that interlocks with methods exposed by HTTPClient
func HTTPRemoteHandler(ds *Dsync) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			info := &dag.Info{}
			if err := json.NewDecoder(r.Body).Decode(info); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
			r.Body.Close()

			if info == nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("body must be a json dag info object"))
				return
			}

			pinOnComplete := r.FormValue("pin") == "true"
			meta := map[string]string{}
			for key := range r.URL.Query() {
				if key != "pin" {
					meta[key] = r.URL.Query().Get(key)
				}
			}

			sid, diff, err := ds.NewReceiveSession(info, pinOnComplete, meta)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Set(httpProtcolIDHeader, string(DsyncProtocolID))
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

			res := ds.ReceiveBlock(r.FormValue("sid"), r.FormValue("hash"), data)

			if res.Status == StatusErrored {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(res.Err.Error()))
			} else if res.Status == StatusRetry {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(res.Err.Error()))
			} else {
				w.WriteHeader(http.StatusOK)
			}
		case "PATCH":
			if err := ds.ReceiveBlocks(r.Context(), r.FormValue("sid"), r.Body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
			w.WriteHeader(http.StatusOK)
		case "GET":
			mfstID := r.FormValue("manifest")
			blockID := r.FormValue("block")
			if mfstID == "" && blockID == "" {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("either manifest or block query params are required"))
			} else if mfstID != "" {

				meta := map[string]string{}
				for key := range r.URL.Query() {
					if key != "manifest" {
						meta[key] = r.URL.Query().Get(key)
					}
				}

				mfst, err := ds.GetDagInfo(r.Context(), mfstID, meta)
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
				data, err := ds.GetBlock(r.Context(), blockID)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Write(data)
			}
		case "DELETE":
			cid := r.FormValue("cid")
			meta := map[string]string{}
			for key := range r.URL.Query() {
				if key != "cid" {
					meta[key] = r.URL.Query().Get(key)
				}
			}

			if err := ds.RemoveCID(r.Context(), cid, meta); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			w.WriteHeader(http.StatusOK)
		}
	}
}
