// Package dsync implements point-to-point merkle-DAG-syncing between a local 
// instance and remote source. It's like rsync, but specific to merkle-DAGs.
// dsync operates over HTTP and (soon) libp2p connections.
//
// dsync by default can push & fetch DAGs to another dsync instance, called
// the "remote". Dsync instances that want to accept merkle-DAGs must opt into 
// operating as a remote by configuring a dsync.Dsync instance to do so
// 
// Dsync is structured as bring-your-own DAG vetting. All push requests are 
// run through two "check" functions called at the beginning and and of the
// push process. Each check function supplies details about the push being
// requested or completed. The default intial check function rejects all 
// requests, and must be overridden to accept data
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

// Remote is a source that can be synced to & from. dsync requests automate
// calls to this interface with higher-order functions like Push and Pull
type Remote interface {
	// NewReceiveSession requests a new send session from the remote, which will return a
	// delta manifest of blocks the remote needs and a session id that must
	// be sent with each block
	NewReceiveSession(info *dag.Info, pinOnComplete bool) (sid string, diff *dag.Manifest, err error)
	// ReceiveBlock places a block on the remote
	ReceiveBlock(sid, hash string, data []byte) ReceiveResponse

	// GetDagInfo asks the remote for info specified by a the root identifier 
	// string of a DAG
	GetDagInfo(ctx context.Context, cidStr string) (info *dag.Info, err error)
	// GetBlock gets a block of data from the remote
	GetBlock(ctx context.Context, hash string) (rawdata []byte, err error)
}

// DagPreCheck is a function that a remote will run before beginning a transfer.
// If DagCheck returns an error, the remote will deny the request to push any 
// blocks, no session will be created and the error message will be returned 
// in the response status. DagPreCheck is the right to place to implement
// peerID and contentID accept/reject lists.
// 
// DagPreCheck can also reject based on size limitations by examining data
// provided by the requester, but be advised the Info provided is gossip at
// this point in the sync process.
type DagPreCheck func(context.Context, dag.Info) error

// DefaultDagPrecheck rejects all requests
var DefaultDagPrecheck = func(context.Context, dag.Info) error {
	return fmt.Errorf("remote is not configured to accept DAGs")
}

// DagFinalCheck is a function remote will call occurs after the requested push 
// has transferred blocks to the remote, giving the remote a chance to work with
// the data that's been sent before making a final decision on weather or not
// to keep the data in question. If DagFinalCheck returns an error the transfer
// is halted, returning the error message to the remote. 
// any request to pin the data will be skipped.
// TODO (b5): blocks pushed by a rejected transfer must be explicitly removed
type DagFinalCheck func(context.Context, dag.Info) error

// DefaultDagFinalCheck by default performs no check
var DefaultDagFinalCheck = func(context.Context, dag.Info) error {
	return nil
}

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


	// preCheck is called before creating a receive session
	preCheck DagPreCheck
	// dagFinalCheck is called before finalizing a receive session
	finalCheck DagFinalCheck
	// http server accepting dsync requests
	httpServer *http.Server
	requireAllBlocks bool
	// inbound transfers in progress, will be nil if not acting as a remote
	sessionLock    sync.Mutex
	sessionPool    map[string]*session
	sessionCancels map[string]context.CancelFunc
	sessionTTLDur  time.Duration
}

// compile-time assertion that Dsync satisfies the remote interface
var _ Remote = (*Dsync)(nil)

// Config encapsulates optional Dsync configuration
type Config struct {
	// InfoStore is an optional caching layer for dag.Info objects
	InfoStore         dag.InfoStore
	// provide a listening addres to have Dsync spin up an HTTP server when
	// StartRemote(ctx) is called
	HTTPRemoteAddress string
	// PinAPI is required for remotes to accept 
	PinAPI            coreiface.PinAPI
	// User-Supplied PreCheck function for a remote accepting DAGs
	PreCheck          DagPreCheck
	// User-Supplied Final check function for a remote accepting DAGs
	FinalCheck DagFinalCheck
	// RequireAllBlocks will skip checking for blocks already present on the
	// remote, requiring push requests to send all blocks each time
	ReqiureAllBlocks bool
}

// Validate confirms the configuration is valid
func (cfg *Config) Validate() error {
	if cfg.PreCheck == nil {
		return fmt.Errorf("PreCheck is required")
	}
	if cfg.FinalCheck == nil {
		return fmt.Errorf("FinalCheck is required")
	}
	return nil
}

// New creates a local Dsync service. By default Dsync can push and pull to
// remotes. It can be configured to act as a remote for other Dsync instances
func New(localNodes ipld.NodeGetter, blockStore coreiface.BlockAPI, opts ...func(cfg *Config)) (*Dsync, error) {
	cfg := &Config{
		PreCheck: DefaultDagPrecheck,
		FinalCheck: DefaultDagFinalCheck,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	ds := &Dsync{
		lng:  localNodes,
		bapi: blockStore,

		preCheck: cfg.PreCheck,
		finalCheck: cfg.FinalCheck,
		sessionPool:    map[string]*session{},
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

	return ds, nil
}

// StartRemote makes dsync available for remote requests, starting an HTTP 
// server if a listening address is specified.
// StartRemote returns immediately. Stop remote service by cancelling 
// the passed-in context.
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

	info, err := dag.NewInfo(context.Background(), ds.lng, id)
	if err != nil {
		return nil, err
	}

	return ds.NewPushInfo(info, remoteAddr, pinOnComplete)
}

// NewPushInfo creates a push from an existing dag.Info. All blocks in the
// info manifest must be accessible from the local Dsync block repository
func (ds *Dsync) NewPushInfo(info *dag.Info, remoteAddr string, pinOnComplete bool) (*Push, error) {
	rem := &HTTPClient{URL: remoteAddr}
	return NewPush(ds.lng, info, rem, pinOnComplete)
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
func (ds *Dsync) NewReceiveSession(info *dag.Info, pinOnComplete bool) (sid string, diff *dag.Manifest, err error) {

	// TODO (b5) - figure out context passing
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(ds.sessionTTLDur))

	if err = ds.preCheck(ctx, *info); err != nil {
		return
	}

	if pinOnComplete && ds.pin == nil {
		err = fmt.Errorf("remote doesn't support pinning")
		return
	}

	sess, err := newSession(ctx, ds.lng, ds.bapi, info, !ds.requireAllBlocks, pinOnComplete)
	if err != nil {
		cancel()
		return
	}
	fmt.Printf("created receive push session. sid: %s. diff: %d nodes\n", sess.id, len(sess.diff.Nodes))

	ds.sessionLock.Lock()
	defer ds.sessionLock.Unlock()
	ds.sessionPool[sess.id] = sess
	ds.sessionCancels[sess.id] = cancel

	return sess.id, sess.diff, nil
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

	// if we're done transferring, finalize!
	if res.Status == StatusOk && sess.Complete() {
		if err := ds.finalizeReceive(sess); err != nil {
			return ReceiveResponse{
				Hash: sess.info.RootCID().String(),
				Status: StatusErrored,
				Err: err,
			}
		}
	}

	return res
}

// TODO (b5): needs to be called if someone tries to sync a DAG that requires
// no blocks for an early termination, ensuring that we cache a dag.Info in 
// that case as well
func (ds *Dsync) finalizeReceive(sess *session) error {
		if err := ds.finalCheck(sess.ctx, *sess.info); err != nil {
			return err
		}

		if ds.infoStore != nil {
			di := sess.info
			if err := ds.infoStore.PutDAGInfo(sess.ctx, sess.info.Manifest.Nodes[0], di); err != nil {
				return err
			}
		}

		if sess.pin {
			if err := ds.pin.Add(sess.ctx, path.New(sess.info.Manifest.Nodes[0])); err != nil {
				return err
			}
		}

		defer func() {
			ds.sessionLock.Lock()
			ds.sessionCancels[sess.id]()
			delete(ds.sessionPool, sess.id)
			ds.sessionLock.Unlock()
		}()

		return nil
}

// GetDagInfo gets the manifest for a DAG rooted at id, checking any configured cache before falling back to generating a new manifest
func (ds *Dsync) GetDagInfo(ctx context.Context, hash string) (info *dag.Info, err error) {
	// check cache if one is specified
	if ds.infoStore != nil {
		if info, err = ds.infoStore.DAGInfo(ctx, hash); err == nil {
			fmt.Println("using cached manifest")
			return
		}
	}

	id, err := cid.Parse(hash)
	if err != nil {
		return nil, err
	}

	return dag.NewInfo(ctx, ds.lng, id)
}

// GetBlock returns a single block from the store
func (ds *Dsync) GetBlock(ctx context.Context, hash string) ([]byte, error) {
	rdr, err := ds.bapi.Get(ctx, path.New(hash))
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(rdr)
}
