package dsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"

	"github.com/qri-io/dag"

	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
	coreiface "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface"
)

// ResponseStatus defines types of results for a request
type ResponseStatus int

const (
	// StatusErrored indicates the request failed and cannot be retried
	StatusErrored ResponseStatus = -1
	// StatusOk indicates the request completed successfully
	StatusOk ResponseStatus = 0
	// StatusRetry indicates the request can be attempted again
	StatusRetry ResponseStatus = 1
)

// Remote is an interface for a source that can be synced to & from
type Remote interface {
	// ReqSend requests a new send session from the remote, which will return a
	// delta manifest of blocks the remote needs and a session id that must
	// be sent with each block
	ReqSend(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error)
	// PutBlock places a block on the remote
	PutBlock(sid, hash string, data []byte) Response

	// ReqManifest asks the remote for a manifest specified by the root ID of a DAG
	ReqManifest(ctx context.Context, path string) (mfst *dag.Manifest, err error)
	// GetBlock gets a block from the remote
	GetBlock(ctx context.Context, hash string) (rawdata []byte, err error)
}

// Response defines the result of sending a block, or attempting to send a block
// TODO (b5): rename to SendResponse
type Response struct {
	Hash   string
	Status ResponseStatus
	Err    error
}

// HTTPRemote implents the Remote interface via HTTP requests
type HTTPRemote struct {
	URL string
}

// ReqSend initiates a send session
func (rem *HTTPRemote) ReqSend(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error) {
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
	// TODO (b5): this is really terrible to print here, but is *very* helpful info on the CLI
	// we should pipe a completion channel up to the CLI & remove this
	fmt.Printf("   sending %d/%d blocks (session id: %s)\n", len(diff.Nodes), len(mfst.Nodes), sid)
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

// ReqManifest gets a
func (rem *HTTPRemote) ReqManifest(ctx context.Context, id string) (mfst *dag.Manifest, err error) {
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

// GetBlock fetches a block from HTTPRemote
func (rem *HTTPRemote) GetBlock(ctx context.Context, id string) (data []byte, err error) {
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

// Receive tracks state of receiving a manifest of blocks from a remote
// TODO (b5): This is session state, and should be renamed to reflect that. ReceiveSession? PushState?
type Receive struct {
	sid    string
	ctx    context.Context
	lng    ipld.NodeGetter
	bapi   coreiface.BlockAPI
	mfst   *dag.Manifest
	diff   *dag.Manifest
	prog   dag.Completion
	progCh chan dag.Completion
}

// NewReceive creates a receive state machine
func NewReceive(ctx context.Context, lng ipld.NodeGetter, bapi coreiface.BlockAPI, mfst *dag.Manifest) (*Receive, error) {
	// TODO (b5): ipfs api/v0/get/block doesn't allow checking for local blocks yet
	// aren't working over ipfs api, so we can't do delta's quite yet. Just send the whole things back
	diff := mfst

	// diff, err := dag.Missing(ctx, lng, mfst)
	// if err != nil {
	// 	return nil, err
	// }

	r := &Receive{
		sid:    randStringBytesMask(10),
		ctx:    ctx,
		lng:    lng,
		bapi:   bapi,
		mfst:   mfst,
		diff:   diff,
		prog:   dag.NewCompletion(mfst, diff),
		progCh: make(chan dag.Completion),
	}

	go r.completionChanged()

	return r, nil
}

// ReceiveBlock accepts a block from the sender, placing it in the local blockstore
func (r *Receive) ReceiveBlock(hash string, data io.Reader) Response {
	bstat, err := r.bapi.Put(r.ctx, data)

	if err != nil {
		return Response{
			Hash:   hash,
			Status: StatusRetry,
			Err:    err,
		}
	}

	id := bstat.Path().Cid()
	if id.String() != hash {
		return Response{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("hash mismatch. expected: '%s', got: '%s'", hash, id.String()),
		}
	}

	// this should be the only place that modifies progress
	for i, h := range r.mfst.Nodes {
		if hash == h {
			r.prog[i] = 100
		}
	}
	go r.completionChanged()

	return Response{
		Hash:   hash,
		Status: StatusOk,
	}
}

// Complete returns if this receive session is finished or not
func (r *Receive) Complete() bool {
	return r.prog.Complete()
}

func (r *Receive) completionChanged() {
	r.progCh <- r.prog
}

// the best stack overflow answer evaarrr: https://stackoverflow.com/a/22892986/9416066
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randStringBytesMask(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
