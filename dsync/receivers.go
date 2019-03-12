package dsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/qri-io/dag"

	"gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
	coreiface "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface"
)

// NewReceivers allocates a Receivers pointer
func NewReceivers(ctx context.Context, lng ipld.NodeGetter, bapi coreiface.BlockAPI) *Receivers {
	return &Receivers{
		ctx:     ctx,
		lng:     lng,
		bapi:    bapi,
		pool:    map[string]*Receive{},
		cancels: map[string]context.CancelFunc{},

		TTLDur: time.Hour * 5,
	}
}

// Receivers keeps a pool of receive sessions for serving as a remote to requesters
type Receivers struct {
	ctx       context.Context
	lng       ipld.NodeGetter
	bapi      coreiface.BlockAPI
	infoStore dag.InfoStore

	lock    sync.Mutex
	pool    map[string]*Receive
	cancels map[string]context.CancelFunc

	TTLDur time.Duration
}

// SetInfoStore assigns Receivers InfoStore, which it uses to lookup cached manifests
func (rs *Receivers) SetInfoStore(is dag.InfoStore) {
	rs.infoStore = is
}

// ReqSend takes a manifest sent by a remote and initiates a receive session
// It returns a manifest/diff of the blocks the reciever needs to have a complete DAG
func (rs *Receivers) ReqSend(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error) {
	ctx, cancel := context.WithDeadline(rs.ctx, time.Now().Add(rs.TTLDur))
	r, err := NewReceive(ctx, rs.lng, rs.bapi, mfst)
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

// PutBlock adds one block to the local node that was sent by the remote node
// It notes in the Receive which nodes have been added
// When the DAG is complete, it puts the manifest into a DAG info and the
// DAG info into an infoStore
func (rs *Receivers) PutBlock(sid, hash string, data []byte) Response {
	r, ok := rs.pool[sid]
	if !ok {
		return Response{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("sid not found"),
		}
	}

	// ReceiveBlock accepts a block from the sender, placing it in the local blockstore
	res := r.ReceiveBlock(hash, bytes.NewReader(data))

	if res.Status == StatusOk && r.Complete() {
		// TODO (b5): move this into a "finalizeReceive" method on receivers
		// This code will also need to be called if someone tries to sync a DAG that requires
		// no blocks for an early termination, ensuring that we cache a dag.Info in that case
		// as well
		if rs.infoStore != nil {
			di := &dag.Info{
				Manifest: r.mfst,
			}
			if err := rs.infoStore.PutDAGInfo(context.Background(), r.mfst.Nodes[0], di); err != nil {
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

// ReqManifest gets the manifest for a DAG rooted at id, checking any configured cache before falling back to generating a new manifest
func (rs *Receivers) ReqManifest(ctx context.Context, hash string) (mfst *dag.Manifest, err error) {
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

// GetBlock returns a single block from the store
func (rs *Receivers) GetBlock(ctx context.Context, hash string) ([]byte, error) {
	path, err := coreiface.ParsePath(hash)
	if err != nil {
		return nil, err
	}

	rdr, err := rs.bapi.Get(ctx, path)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(rdr)
}

// HTTPHandler exposes Receivers over HTTP, interlocks with methods exposed by HTTPRemote
func (rs *Receivers) HTTPHandler() http.HandlerFunc {
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

			sid, diff, err := rs.ReqSend(mfst)
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

			res := rs.PutBlock(r.FormValue("sid"), r.FormValue("hash"), data)

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
				mfst, err := rs.ReqManifest(r.Context(), mfstID)
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
				data, err := rs.GetBlock(r.Context(), blockID)
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
