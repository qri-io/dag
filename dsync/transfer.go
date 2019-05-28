package dsync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/qri-io/dag"
)

// ResponseStatus defines types of results for a request
type ResponseStatus int

const (
	// StatusErrored igondicates the request failed and cannot be retried
	StatusErrored ResponseStatus = -1
	// StatusOk indicates the request completed successfully
	StatusOk ResponseStatus = 0
	// StatusRetry indicates the request can be attempted again
	StatusRetry ResponseStatus = 1
)

// Response defines the result of sending a block, or attempting to send a block.
// TODO (b5): rename to SendResponse
type Response struct {
	Hash   string
	Status ResponseStatus
	Err    error
}

// Transfer tracks the state of receiving a manifest of blocks from a remote.
type Transfer struct {
	sid    string
	ctx    context.Context
	lng    ipld.NodeGetter
	bapi   coreiface.BlockAPI
	mfst   *dag.Manifest
	diff   *dag.Manifest
	prog   dag.Completion
	progCh chan dag.Completion
}

// NewTransfer creates a receive state machine
func NewTransfer(ctx context.Context, lng ipld.NodeGetter, bapi coreiface.BlockAPI, mfst *dag.Manifest) (*Transfer, error) {
	// TODO (b5): ipfs api/v0/get/block doesn't allow checking for local blocks yet
	// aren't working over ipfs api, so we can't do delta's quite yet. Just send the whole things back
	diff := mfst

	// diff, err := dag.Missing(ctx, lng, mfst)
	// if err != nil {
	// 	return nil, err
	// }

	r := &Transfer{
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
func (r *Transfer) ReceiveBlock(hash string, data io.Reader) Response {
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
func (r *Transfer) Complete() bool {
	return r.prog.Complete()
}

func (r *Transfer) completionChanged() {
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

// NewTransfers allocates a Transfers pointer
func NewTransfers(ctx context.Context, lng ipld.NodeGetter, bapi coreiface.BlockAPI) *Transfers {
	return &Transfers{
		ctx:     ctx,
		lng:     lng,
		bapi:    bapi,
		pool:    map[string]*Transfer{},
		cancels: map[string]context.CancelFunc{},

		TTLDur: time.Hour * 5,
	}
}

// Transfers keeps a pool of transfer sessions for serving as a remote to requesters
type Transfers struct {
	ctx       context.Context
	lng       ipld.NodeGetter
	bapi      coreiface.BlockAPI
	infoStore dag.InfoStore

	lock    sync.Mutex
	pool    map[string]*Transfer
	cancels map[string]context.CancelFunc

	TTLDur time.Duration
}

// SetInfoStore assigns Transfers InfoStore, which it uses to lookup cached manifests
func (rs *Transfers) SetInfoStore(is dag.InfoStore) {
	rs.infoStore = is
}

// PushStart takes a manifest sent by a remote and initiates a transfer session
// It returns a manifest/diff of the blocks the reciever needs to have a complete DAG
func (rs *Transfers) PushStart(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error) {
	ctx, cancel := context.WithDeadline(rs.ctx, time.Now().Add(rs.TTLDur))
	r, err := NewTransfer(ctx, rs.lng, rs.bapi, mfst)
	if err != nil {
		cancel()
		return
	}
	fmt.Printf("created receive. sid: %s. diff: %d nodes\n", r.sid, len(r.diff.Nodes))

	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.pool[r.sid] = r
	rs.cancels[r.sid] = cancel

	return r.sid, r.diff, nil
}

// PushBlock adds one block to the local node that was sent by the remote node
// It notes in the Receive which nodes have been added
// When the DAG is complete, it puts the manifest into a DAG info and the
// DAG info into an infoStore
func (rs *Transfers) PushBlock(sid, hash string, data []byte) Response {
	t, ok := rs.pool[sid]
	if !ok {
		return Response{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("sid not found"),
		}
	}

	// ReceiveBlock accepts a block from the sender, placing it in the local blockstore
	res := t.ReceiveBlock(hash, bytes.NewReader(data))

	if res.Status == StatusOk && t.Complete() {
		// TODO (b5): move this into a "finalizeReceive" method on receivers
		// This code will also need to be called if someone tries to sync a DAG that requires
		// no blocks for an early termination, ensuring that we cache a dag.Info in that case
		// as well
		if rs.infoStore != nil {
			di := &dag.Info{
				Manifest: t.mfst,
			}
			if err := rs.infoStore.PutDAGInfo(context.Background(), t.mfst.Nodes[0], di); err != nil {
				return Response{
					Hash:   hash,
					Status: StatusErrored,
					Err:    err,
				}
			}
		}
		defer func() {
			rs.lock.Lock()
			rs.cancels[sid]()
			delete(rs.pool, sid)
			rs.lock.Unlock()
		}()
	}

	return res
}

// PullManifest gets the manifest for a DAG rooted at id, checking any configured cache before falling back to generating a new manifest
func (rs *Transfers) PullManifest(ctx context.Context, hash string) (mfst *dag.Manifest, err error) {
	// check cache if one is specified
	if rs.infoStore != nil {
		var di *dag.Info
		if di, err = rs.infoStore.DAGInfo(ctx, hash); err == nil {
			fmt.Println("using cached manifest")
			mfst = di.Manifest
			return
		}
	}

	id, err := cid.Parse(hash)
	if err != nil {
		return nil, err
	}

	return dag.NewManifest(ctx, rs.lng, id)
}

// PullBlock returns a single block from the store
func (rs *Transfers) PullBlock(ctx context.Context, hash string) ([]byte, error) {
	rdr, err := rs.bapi.Get(ctx, path.New(hash))
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(rdr)
}
