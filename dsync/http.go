package dsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	format "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/qri-io/dag"
)

const (
	httpProtcolIDHeader = "dsync-version"
	carArchiveMediaType = "archive/car"
	cborMediaType       = "application/cbor"
	sidHeader           = "sid"
)

// HTTPClient is the request side of doing dsync over HTTP
type HTTPClient struct {
	URL           string
	NodeGetter    format.NodeGetter
	BlockAPI      coreiface.BlockAPI
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

	req, err := http.NewRequest(http.MethodPost, u.String(), buf)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpProtcolIDHeader, string(DsyncProtocolID))

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

	rem.remProtocolID = protocolIDFromHTTPData(req.URL, res.Header)

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

// ReceiveBlocks writes a block stream as an HTTP PUT request to the remote
func (rem *HTTPClient) ReceiveBlocks(ctx context.Context, sid string, r io.Reader) error {

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s?sid=%s", rem.URL, sid), r)
	if err != nil {
		log.Debugf("err creating %s HTTP request err=%q ", http.MethodPut, err)
		return err
	}
	req.TransferEncoding = []string{"chunked"}
	req.Header.Set("Content-Type", carArchiveMediaType)
	req.Header.Set(httpProtcolIDHeader, string(DsyncProtocolID))

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

// ReceiveBlock asks a remote to receive a block over HTTP
func (rem *HTTPClient) ReceiveBlock(sid, hash string, data []byte) ReceiveResponse {
	url := fmt.Sprintf("%s?sid=%s&hash=%s", rem.URL, sid, hash)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(data))
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

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
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
	req, err := http.NewRequest(http.MethodGet, url, nil)
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

// OpenBlockStream sends a dag.Info to the remote & asks that it returns a
// stream of blocks in the info's manifest
func (rem *HTTPClient) OpenBlockStream(ctx context.Context, info *dag.Info, meta map[string]string) (io.ReadCloser, error) {
	u, err := url.Parse(rem.URL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	for key, value := range meta {
		q.Add(key, value)
	}
	u.RawQuery = q.Encode()

	bodyData, err := info.MarshalCBOR()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPatch, u.String(), bytes.NewBuffer(bodyData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", cborMediaType)
	req.Header.Set("Accept", carArchiveMediaType)
	req.Header.Set(httpProtcolIDHeader, string(DsyncProtocolID))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("unexpected HTTP response: %d: %q", res.StatusCode, string(body))
	}

	if res.Header.Get("Content-Type") != carArchiveMediaType {
		return nil, fmt.Errorf("unexpected media type: %s", res.Header.Get("Content-Type"))
	}

	return res.Body, nil
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

	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
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
		w.Header().Set(httpProtcolIDHeader, string(DsyncProtocolID))

		switch r.Method {
		case http.MethodPost:
			createDsyncSession(ds, w, r)
		case http.MethodPut:
			if r.Header.Get("Content-Type") == carArchiveMediaType {
				if err := ds.ReceiveBlocks(r.Context(), r.FormValue("sid"), r.Body); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(err.Error()))
					return
				}
				w.WriteHeader(http.StatusOK)
				return
			}

			receiveBlockHTTP(ds, w, r)
		case http.MethodGet:
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
		case http.MethodPatch:
			meta := map[string]string{}
			for key := range r.URL.Query() {
				meta[key] = r.URL.Query().Get(key)
			}

			info, err := decodeDAGInfoBody(r)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
			r, err := ds.OpenBlockStream(r.Context(), info, meta)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Set("Content-Type", carArchiveMediaType)
			w.WriteHeader(http.StatusOK)
			defer r.Close()
			io.Copy(w, r)
			return

		case http.MethodDelete:
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

func protocolIDFromHTTPData(url *url.URL, headers http.Header) protocol.ID {
	protocolIDHeaderStr := headers.Get(httpProtcolIDHeader)
	if protocolIDHeaderStr == "" {
		// protocol ID header only exists in version 0.2.0 and up, when header isn't
		// present assume version 0.1.1, the latest version before header was set
		// 0.1.1 is wire-compatible with all lower versions of dsync
		return protocol.ID("/dsync/0.1.1")
	}

	return protocol.ID(protocolIDHeaderStr)
}

func decodeDAGInfoBody(r *http.Request) (*dag.Info, error) {
	defer r.Body.Close()
	info := &dag.Info{}

	switch r.Header.Get("Content-Type") {
	case cborMediaType:
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		info, err = dag.UnmarshalCBORDagInfo(data)
		if err != nil {
			return nil, err
		}
	default:
		// default to JSON for legacy reads
		err := json.NewDecoder(r.Body).Decode(info)
		if err != nil {
			return nil, err
		}
	}

	if info.Manifest == nil {
		return nil, fmt.Errorf("body must be a json dag info object")
	}

	return info, nil
}

func receiveBlockHTTP(ds *Dsync, w http.ResponseWriter, r *http.Request) {
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
}

func createDsyncSession(ds *Dsync, w http.ResponseWriter, r *http.Request) {
	info, err := decodeDAGInfoBody(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
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

	w.Header().Set(sidHeader, sid)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(diff)
}
