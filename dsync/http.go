package dsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/qri-io/dag"
)

const (
	httpDsyncProtocolIDHeader  = "dsync-version"
	sidHeader                  = "sid"
	httpBlocksMultipartFormKey = "blocks"
)

const (
	carMIMEType    = "archive/car"
	cborMIMEType   = "application/cbor"
	jsonMIMEType   = "application/json"
	binaryMIMEType = "application/octet-stream"
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
	req.Header.Set("Content-Type", jsonMIMEType)
	req.Header.Set("Accept", jsonMIMEType)
	req.Header.Set(httpDsyncProtocolIDHeader, string(DsyncProtocolID))

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
func (rem *HTTPClient) ReceiveBlocks(ctx context.Context, sid string, carReader io.Reader) error {
	pipeR, pipeW := io.Pipe()
	m := multipart.NewWriter(pipeW)
	go func() {
		defer pipeW.Close()
		defer m.Close()

		part, err := m.CreateFormFile(httpBlocksMultipartFormKey, "archive.car")
		if err != nil {
			log.Debugw("error creating form file", "err", err)
			return
		}

		n, err := io.Copy(part, carReader)
		if err != nil {
			log.Debugw("error copying car reader to multipart form file", "err", err)
			return
		}
		log.Debugf("done writing form file. copied %d bytes", n)
	}()

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s?sid=%s", rem.URL, sid), pipeR)
	if err != nil {
		log.Debugw("error creating HTTP request", "err", err)
		return err
	}
	req.Header.Set("Content-Type", m.FormDataContentType())
	// response body is only used for error reporting
	req.Header.Set("Accept", binaryMIMEType)
	req.Header.Set(httpDsyncProtocolIDHeader, string(DsyncProtocolID))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Debugw("error doing HTTP request", "err", err)
		return err
	}

	if res.StatusCode != http.StatusOK {
		var msg string
		if data, err := ioutil.ReadAll(res.Body); err == nil {
			msg = string(data)
		}
		log.Debugw("error response from remote", "response", msg)
		return fmt.Errorf("remote response: %d %s", res.StatusCode, msg)
	}

	return nil
}

// ReceiveBlock asks a remote to receive a block over HTTP
func (rem *HTTPClient) ReceiveBlock(sid string, id cid.Cid, data []byte) ReceiveResponse {
	url := fmt.Sprintf("%s?sid=%s&hash=%s", rem.URL, sid, id)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(data))
	if err != nil {
		log.Debugf("http client create request error=%s", err)
		return ReceiveResponse{
			Cid:    id,
			Status: StatusErrored,
			Err:    err,
		}
	}
	req.Header.Set("Content-Type", binaryMIMEType)
	// response body is only used for error reporting
	req.Header.Set("Accept", binaryMIMEType)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Debugf("http client perform request error=%s", err)
		return ReceiveResponse{
			Cid:    id,
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
			Cid:    id,
			Status: StatusErrored,
			Err:    fmt.Errorf("remote error: %d %s", res.StatusCode, msg),
		}
	}

	return ReceiveResponse{
		Cid:    id,
		Status: StatusOk,
	}
}

// GetDagInfo fetches a manifest from a remote source over HTTP
func (rem *HTTPClient) GetDagInfo(ctx context.Context, id cid.Cid, meta map[string]string) (info *dag.Info, err error) {
	u, err := url.Parse(rem.URL)
	if err != nil {
		return
	}
	q := u.Query()
	q.Set("manifest", id.String())
	for key, val := range meta {
		q.Set(key, val)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", jsonMIMEType)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	rem.remProtocolID = protocolIDFromHTTPData(req.URL, res.Header)

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
func (rem *HTTPClient) GetBlock(ctx context.Context, id cid.Cid) (data []byte, err error) {
	url := fmt.Sprintf("%s?block=%s", rem.URL, id)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", binaryMIMEType)

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
	req.Header.Set("Content-Type", cborMIMEType)
	req.Header.Set("Accept", carMIMEType)
	req.Header.Set(httpDsyncProtocolIDHeader, string(DsyncProtocolID))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("unexpected HTTP response: %d: %q", res.StatusCode, string(body))
	}

	if res.Header.Get("Content-Type") != carMIMEType {
		return nil, fmt.Errorf("unexpected media type: %s", res.Header.Get("Content-Type"))
	}

	return res.Body, nil
}

// RemoveCID asks a remote to remove a CID
func (rem *HTTPClient) RemoveCID(ctx context.Context, id cid.Cid, meta map[string]string) (err error) {
	u, err := url.Parse(rem.URL)
	if err != nil {
		return
	}
	q := u.Query()
	q.Set("cid", id.String())
	for key, val := range meta {
		q.Set(key, val)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return err
	}
	// response body is only used for error reporting
	req.Header.Set("Accept", binaryMIMEType)

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
		w.Header().Set(httpDsyncProtocolIDHeader, string(DsyncProtocolID))

		switch r.Method {
		case http.MethodPost:
			createDsyncSession(ds, w, r)
		case http.MethodPut:
			mtype, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil {
				log.Debugw("bad media type", "err", err)
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			switch mtype {
			case "multipart/form-data":
				log.Debugw("multipart params", "params", params, "sid", r.FormValue("sid"))
				defer r.Body.Close()

				prt, header, err := r.FormFile(httpBlocksMultipartFormKey)
				if err != nil {
					log.Debugw("error getting form file", "err", err)
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(err.Error()))
					return
				}

				log.Debugw("reading block data", "prt", prt, "header", header)
				if err := ds.ReceiveBlocks(r.Context(), r.FormValue("sid"), prt); err != nil {
					log.Debugw("error reading multipart block stream", "err", err)
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(err.Error()))
				}
				w.WriteHeader(http.StatusOK)
				return

			case carMIMEType:
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

				mfstCid, err := cid.Parse(mfstID)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(err.Error()))
					return
				}

				meta := map[string]string{}
				for key := range r.URL.Query() {
					if key != "manifest" {
						meta[key] = r.URL.Query().Get(key)
					}
				}

				mfst, err := ds.GetDagInfo(r.Context(), mfstCid, meta)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(err.Error()))
					return
				}

				data, err := json.Marshal(mfst)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
					return
				}

				w.Header().Set("Content-Type", jsonMIMEType)
				w.Write(data)
			} else {
				blockCid, err := cid.Parse(blockID)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(err.Error()))
					return
				}

				data, err := ds.GetBlock(r.Context(), blockCid)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
					return
				}
				w.Header().Set("Content-Type", binaryMIMEType)
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

			w.Header().Set("Content-Type", carMIMEType)
			w.WriteHeader(http.StatusOK)
			defer r.Close()
			io.Copy(w, r)
			return

		case http.MethodDelete:
			cidStr := r.FormValue("cid")
			id, err := cid.Parse(cidStr)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			meta := map[string]string{}
			for key := range r.URL.Query() {
				if key != "cid" {
					meta[key] = r.URL.Query().Get(key)
				}
			}

			if err := ds.RemoveCID(r.Context(), id, meta); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			w.WriteHeader(http.StatusOK)
		}
	}
}

func protocolIDFromHTTPData(url *url.URL, headers http.Header) protocol.ID {
	protocolIDHeaderStr := headers.Get(httpDsyncProtocolIDHeader)
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
	case cborMIMEType:
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

	resCid, err := cid.Parse(r.FormValue("hash"))
	if err != nil {
		log.Debugw("error parsing HTTP receive-block cid", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("well-formed cid 'hash' value is required"))
		return
	}

	res := ds.ReceiveBlock(r.FormValue("sid"), resCid, data)

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
