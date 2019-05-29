// Package dsync implements point-to-point block-syncing between a local and remote source.
// It's like rsync, but specific to merkle-dags
//
// How Dsync Works On The Local Side
//
// Fetch
//
// The local creates a fetch using `NewFetch` (or `NewFetchWithManifest`, if the local already has the Manifest of the DAG at `id`).
//
//  The local starts the fetch process by using `fetch.Do()`. It:
//
//  1) requests the Manifest of the DAG at `id`
//  2) determines which blocks it needs to ask the remote for, and creates a diff Manifest (note: right now, because of
//     the way we are able to access the block level storage, we can't actually produce a meaningful diff, so the diff is
//     actually just the full Manifest)
//  3) creates a number of fetchers to fetch the blocks in parallel.
//  4) sets up processes to coordinate the fetch responses
//
//  Fetch.Do() ends when:
//  - the process has timed out
//  - some response has returned an error
//  - the local has successfully fetched all the blocks needed to complete the DAG
//
// Send
//
// The local has a DAG it wants to send to a remote. The local creates a `send` using `NewSend` and a Manifest.
//
//  The local starts the send process by using `send.Do()`. It:
//
//  1) requests a send using `remote.ReqSend`. It gets back a send id (`sid`) and the `diff` the Manifest. The `diff`
//     Manifest lists all the blocks ids remote needs in order to have the full DAG (note: right now, the diff is always
//     the full Manifest)
//  2) creates a number of senders to send or re-send the blocks in parallel
//  3) sets up processes to coordiate the send responses
//
//  Send.Do() ends when:
//  - the process has timed out
//  - the local has have attempted to many re-tries
//  - some response from the remote returns an error
//  - the local successfully sent the full DAG to the remote
//
// How Dsync Works On The Remote Side
//
// The remote does not keep track of (or coordinate) fetch and it keeps only cursory track of send.
//
// Instead, the local uses the `Remote` interface (`ReqSend`, `PutBlocks`, `ReqManifest`, `GetBlock` methods) to communicate with the remote. Currently, we have an implimentation of the `Remote` interface that communicates over HTTP. We use `HTTPRemote` on the local to send requests and `Receivers` on the remote to handle and respond to them.
//
// `HTTPRemote` structures requests correctly to work with the remote's Receiver http api. And `HTTPHandler` exposes that http api, so it can handle requests from the local.
package dsync

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/qri-io/dag"
)

const (
	// default to parallelism of 3. So far 4 was enough to blow up a std k8s pod running IPFS :(
	defaultPushParallelism = 3
	// default to parallelism of 3
	// TODO (b5): tune this figure
	defaultPullParallelism = 3
	// total number of retries to attempt before send is considered faulty
	// TODO (b5): this number should be retries *per object*, and a much lower
	// number, like 5.
	maxRetries = 25
)

// Remote is a source that can be synced to & from
type Remote interface {
	// NewReceiveSession requests a new send session from the remote, which will return a
	// delta manifest of blocks the remote needs and a session id that must
	// be sent with each block
	NewReceiveSession(mfst *dag.Manifest, pinOnComplete bool) (sid string, diff *dag.Manifest, err error)
	// ReceiveBlock places a block on the remote
	ReceiveBlock(sid, hash string, data []byte) ReceiveResponse

	// GetManifest asks the remote for a manifest specified by the root ID of a DAG
	GetManifest(ctx context.Context, path string) (mfst *dag.Manifest, err error)
	// GetBlock gets a block from the remote
	GetBlock(ctx context.Context, hash string) (rawdata []byte, err error)
}

// DagCheck is a function that a remote will run before accepting a Dag. If
// DagCheck returns an error, the remote will deny the request to push blocks,
// no session will be created and the error message will be returned in the
// response status
type DagCheck func(context.Context, dag.Manifest) error

// Dsync is a service for synchronizing a DAG of blocks between a local & remote
// source
type Dsync struct {
	// cache of dagInfo/manifests
	infoStore dag.InfoStore
	// local node getter
	lng ipld.NodeGetter
	// local block API for placing blocks
	bapi coreiface.BlockAPI
	// api for pinning blocks
	pin coreiface.PinAPI

	// if dagCheck isn't nil, it's called before creating a session.
	dagCheck DagCheck
	// http server accepting dsync requests
	httpServer *http.Server
	// inbound transfers in progress, will be nil if not acting as a remote
	sessionLock    sync.Mutex
	sessionPool    map[string]*Session
	sessionCancels map[string]context.CancelFunc
	sessionTTLDur  time.Duration
}

// compile-time assertion that Dsync satisfies the remote interface
var _ Remote = (*Dsync)(nil)

// Config encapsulates optional Dsync configuration
type Config struct {
	InfoStore         dag.InfoStore
	PinAPI            coreiface.PinAPI
	HTTPRemoteAddress string
	DagCheck          DagCheck
}

// New creates a local Dsync service. By default Dsync can push and pull to
// remotes, and can be configured to act as a remote for other Dsync instances
func New(localNodes ipld.NodeGetter, blockStore coreiface.BlockAPI, opts ...func(cfg *Config)) *Dsync {
	cfg := &Config{}
	for _, opt := range opts {
		opt(cfg)
	}

	ds := &Dsync{
		lng:  localNodes,
		bapi: blockStore,

		sessionPool:    map[string]*Session{},
		sessionCancels: map[string]context.CancelFunc{},
		sessionTTLDur:  time.Hour * 5,
	}

	if cfg.PinAPI != nil {
		ds.pin = cfg.PinAPI
	}
	if cfg.InfoStore != nil {
		ds.infoStore = cfg.InfoStore
	}

	if cfg.HTTPRemoteAddress != "" {
		ds.httpServer = &http.Server{
			Addr:    cfg.HTTPRemoteAddress,
			Handler: HTTPRemoteHandler(ds),
		}
	}

	return ds
}

// StartRemote makes dsync available for remote requests. StartRemote returns
// immediately. Stop remote service by cancelling the passed-in context.
func (ds *Dsync) StartRemote(ctx context.Context) error {
	if ds.httpServer == nil {
		return fmt.Errorf("dsync is not configured as a remote")
	}

	go func() {
		<-ctx.Done()
		if ds.httpServer != nil {
			ds.httpServer.Close()
		}
	}()

	if ds.httpServer != nil {
		go ds.httpServer.ListenAndServe()
	}

	return nil
}

// NewPush creates a push from Dsync to a remote address
func (ds *Dsync) NewPush(cidStr, remoteAddr string, pinOnComplete bool) (*Push, error) {
	id, err := cid.Parse(cidStr)
	if err != nil {
		return nil, err
	}
	mfst, err := dag.NewManifest(context.Background(), ds.lng, id)
	if err != nil {
		return nil, err
	}

	return ds.NewPushManifest(mfst, remoteAddr, pinOnComplete)
}

// NewPushManifest creates a push from an existing manifest. All blocks in the
// manifest must be accessible from the local Dsync block repository
func (ds *Dsync) NewPushManifest(mfst *dag.Manifest, remoteAddr string, pinOnComplete bool) (*Push, error) {
	rem := &HTTPClient{URL: remoteAddr}
	return NewPush(ds.lng, mfst, rem, pinOnComplete)
}

// NewPull creates a pull. A pull fetches an entire DAG from a remote, placing
// it in the local block store
func (ds *Dsync) NewPull(cidStr, remoteAddr string) (*Pull, error) {
	rem := &HTTPClient{URL: remoteAddr}
	return NewPull(cidStr, ds.lng, ds.bapi, rem)
}

// NewReceiveSession takes a manifest sent by a remote and initiates a
// transfer session. It returns a manifest/diff of the blocks the reciever needs
// to have a complete DAG new sessions are created with a deadline for completion
func (ds *Dsync) NewReceiveSession(mfst *dag.Manifest, pinOnComplete bool) (sid string, diff *dag.Manifest, err error) {
	// TODO (b5) - figure out context passing
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(ds.sessionTTLDur))

	if ds.dagCheck != nil {
		if err = ds.dagCheck(ctx, *mfst); err != nil {
			return
		}
	}

	if pinOnComplete && ds.pin == nil {
		err = fmt.Errorf("remote doesn't support pinning")
		return
	}

	sess, err := NewSession(ctx, ds.lng, ds.bapi, mfst, pinOnComplete)
	if err != nil {
		cancel()
		return
	}
	fmt.Printf("created receive push session. sid: %s. diff: %d nodes\n", sess.sid, len(sess.diff.Nodes))

	ds.sessionLock.Lock()
	defer ds.sessionLock.Unlock()
	ds.sessionPool[sess.sid] = sess
	ds.sessionCancels[sess.sid] = cancel

	return sess.sid, sess.diff, nil
}

// ReceiveBlock adds one block to the local node that was sent by the remote
// node It notes in the Receive which nodes have been added
// When the DAG is complete, it puts the manifest into a DAG info and the
// DAG info into an infoStore
func (ds *Dsync) ReceiveBlock(sid, hash string, data []byte) ReceiveResponse {
	sess, ok := ds.sessionPool[sid]
	if !ok {
		return ReceiveResponse{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("sid not found"),
		}
	}

	// ReceiveBlock accepts a block from the sender, placing it in the local blockstore
	res := sess.ReceiveBlock(hash, bytes.NewReader(data))

	if res.Status == StatusOk && sess.Complete() {
		// TODO (b5): move this into a "finalizeReceive" method on receivers
		// This code will also need to be called if someone tries to sync a DAG that requires
		// no blocks for an early termination, ensuring that we cache a dag.Info in that case
		// as well
		if ds.infoStore != nil {
			di := &dag.Info{
				Manifest: sess.mfst,
			}
			if err := ds.infoStore.PutDAGInfo(sess.ctx, sess.mfst.Nodes[0], di); err != nil {
				return ReceiveResponse{
					Hash:   hash,
					Status: StatusErrored,
					Err:    err,
				}
			}
		}

		if sess.pin {
			if err := ds.pin.Add(sess.ctx, path.New(sess.mfst.Nodes[0])); err != nil {
				return ReceiveResponse{
					Hash:   sess.mfst.Nodes[0],
					Status: StatusErrored,
					Err:    err,
				}
			}
		}

		defer func() {
			ds.sessionLock.Lock()
			ds.sessionCancels[sid]()
			delete(ds.sessionPool, sid)
			ds.sessionLock.Unlock()
		}()
	}

	return res
}

// GetManifest gets the manifest for a DAG rooted at id, checking any configured cache before falling back to generating a new manifest
func (ds *Dsync) GetManifest(ctx context.Context, hash string) (mfst *dag.Manifest, err error) {
	// check cache if one is specified
	if ds.infoStore != nil {
		var di *dag.Info
		if di, err = ds.infoStore.DAGInfo(ctx, hash); err == nil {
			fmt.Println("using cached manifest")
			mfst = di.Manifest
			return
		}
	}

	id, err := cid.Parse(hash)
	if err != nil {
		return nil, err
	}

	return dag.NewManifest(ctx, ds.lng, id)
}

// GetBlock returns a single block from the store
func (ds *Dsync) GetBlock(ctx context.Context, hash string) ([]byte, error) {
	rdr, err := ds.bapi.Get(ctx, path.New(hash))
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(rdr)
}
