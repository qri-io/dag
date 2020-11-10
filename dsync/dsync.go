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
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	golog "github.com/ipfs/go-log"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	path "github.com/ipfs/interface-go-ipfs-core/path"
	host "github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/qri-io/dag"
)

var log = golog.Logger("dsync")

func init() {
	golog.SetLogLevel("dsync", "debug")
}

const (
	// DsyncProtocolID is the dsyc p2p Protocol Identifier & version tag
	DsyncProtocolID = protocol.ID("/dsync/0.2.0")
	// default to parallelism of 3. So far 4 was enough to blow up a std k8s pod running IPFS :(
	defaultPushParallelism = 1
	// default to parallelism of 3
	// TODO (b5): tune this figure
	defaultPullParallelism = 1
	// total number of retries to attempt before send is considered faulty
	// TODO (b5): this number should be retries *per object*, and a much lower
	// number, like 5.
	maxRetries = 80
)

var (
	// ErrRemoveNotSupported is the error value returned by remotes that don't
	// support delete operations
	ErrRemoveNotSupported = fmt.Errorf("remove is not supported")
	// ErrUnknownProtocolVersion is the error for when the version of the remote
	// protocol is unknown, usually because the handshake with the the remote
	// hasn't happened yet
	ErrUnknownProtocolVersion = fmt.Errorf("unknown protocol version")
)

// DagSyncable is a source that can be synced to & from. dsync requests automate
// calls to this interface with higher-order functions like Push and Pull
//
// In order to coordinate between a local and a remote, you need something that
// will satisfy the DagSyncable interface on both ends of the wire, one to act
// as the requester and the other to act as the remote
type DagSyncable interface {
	// NewReceiveSession starts a push session from local to a remote.
	// The remote will return a delta manifest of blocks the remote needs
	// and a session id that must be sent with each block
	NewReceiveSession(info *dag.Info, pinOnComplete bool, meta map[string]string) (sid string, diff *dag.Manifest, err error)
	// ProtocolVersion indicates the version of dsync the remote speaks, only
	// available after a handshake is established. Calling this method before a
	// handshake must return ErrUnknownProtocolVersion
	ProtocolVersion() (protocol.ID, error)

	// ReceiveBlock places a block on the remote
	ReceiveBlock(sid, hash string, data []byte) ReceiveResponse
	// GetDagInfo asks the remote for info specified by a the root identifier
	// string of a DAG
	GetDagInfo(ctx context.Context, cidStr string, meta map[string]string) (info *dag.Info, err error)
	// GetBlock gets a block of data from the remote
	GetBlock(ctx context.Context, hash string) (rawdata []byte, err error)
	// RemoveCID asks the remote to remove a cid. Supporting deletes are optional.
	// DagSyncables that don't implement DeleteCID must return
	// ErrDeleteNotSupported
	RemoveCID(ctx context.Context, cidStr string, meta map[string]string) (err error)
}

// Hook is a function that a dsync instance will call at specified points in the
// sync lifecycle
type Hook func(ctx context.Context, info dag.Info, meta map[string]string) error

// DefaultDagPrecheck rejects all requests
// Dsync users are required to override this hook to make dsync work,
// and are expected to supply a trust model in this hook. An example trust model
// is a peerID and contentID accept/reject list supplied by the application
//
// Precheck could also reject based on size limitations by examining data
// provided by the requester, but be advised the Info provided is gossip at
// this point in the sync process.
//
// If the Precheck hook returns an error the remote will deny the request to
// push any blocks, no session will be created and the error message will be
// returned in the response status.
var DefaultDagPrecheck = func(context.Context, dag.Info, map[string]string) error {
	return fmt.Errorf("remote is not configured to accept DAGs")
}

// DefaultDagFinalCheck by default performs no check
var DefaultDagFinalCheck = func(context.Context, dag.Info, map[string]string) error {
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

	// http server accepting dsync requests
	httpServer *http.Server
	// struct for accepting p2p dsync requests
	p2pHandler *p2pHandler

	// requireAllBlocks forces pushes to send *all* blocks,
	// skipping manifest diffing
	requireAllBlocks bool
	// should dsync honor remove requests?
	allowRemoves bool

	// preCheck is called before creating a receive session
	preCheck Hook
	// dagFinalCheck is called before finalizing a receive session
	finalCheck Hook
	// onCompleteHook is optionally called once dag sync is complete
	onCompleteHook Hook
	// getDagInfoCheck is an optional hook to call when a client asks for a dag
	// info
	getDagInfoCheck Hook
	// openBlockStreamCheck is an optional hook to call when a client asks to pull
	// a stream of one or more blocks
	openBlockStreamCheck Hook
	// removeCheck is an optional hook to call before allowing a delete
	removeCheck Hook

	// inbound transfers in progress, will be nil if not acting as a remote
	sessionLock    sync.Mutex
	sessionPool    map[string]*session
	sessionCancels map[string]context.CancelFunc
	sessionTTLDur  time.Duration
}

var (
	// compile-time assertion that Dsync satisfies the remote interface
	_ DagSyncable = (*Dsync)(nil)
	// compile-time assertion that Dsync satisfies streaming interfaces
	_ DagStreamable = (*Dsync)(nil)
)

// Config encapsulates optional Dsync configuration
type Config struct {
	// InfoStore is an optional caching layer for dag.Info objects
	InfoStore dag.InfoStore
	// provide a listening addres to have Dsync spin up an HTTP server when
	// StartRemote(ctx) is called
	HTTPRemoteAddress string
	// to send & push over libp2p connections, provide a libp2p host
	Libp2pHost host.Host
	// PinAPI is required for remotes to accept pinning requests
	PinAPI coreiface.PinAPI

	// RequireAllBlocks will skip checking for blocks already present on the
	// remote, requiring push requests to send all blocks each time
	// This is a helpful override if the receiving node can't distinguish between
	// local and network block access, as with the ipfs-http-api intreface
	RequireAllBlocks bool
	// AllowRemoves let's dsync opt into remove requests. removes are
	// disabled by default
	AllowRemoves bool

	// required check function for a remote accepting DAGs, this hook will be
	// called before a push is allowed to begin
	PushPreCheck Hook
	// optional check function for screening a receive before potentially pinning
	PushFinalCheck Hook
	// optional check function called after successful transfer
	PushComplete Hook
	// optional check to run on dagInfo requests before sending an info back
	GetDagInfoCheck Hook
	// optional hook to run before allowing a stream of blocks
	OpenBlockStreamCheck Hook
	// optional check to run before executing a remove operation
	// the dag.Info given to this check will only contain the root CID being
	// removed
	RemoveCheck Hook
}

// Validate confirms the configuration is valid
func (cfg *Config) Validate() error {
	if cfg.PushPreCheck == nil {
		return fmt.Errorf("PreCheck is required")
	}
	if cfg.PushFinalCheck == nil {
		return fmt.Errorf("FinalCheck is required")
	}
	return nil
}

// OptLibp2pHost is a convenience function for  supplying a libp2p.Host to
// dsync.New
func OptLibp2pHost(host host.Host) func(cfg *Config) {
	return func(cfg *Config) { cfg.Libp2pHost = host }
}

// New creates a local Dsync service. By default Dsync can push and pull to
// remotes. It can be configured to act as a remote for other Dsync instances.
//
// Its crucial that the NodeGetter passed to New be an offline-only getter.
// if using IPFS, this package defines a helper function: NewLocalNodeGetter
// to get an offline-only node getter from an ipfs CoreAPI interface
func New(localNodes ipld.NodeGetter, blockStore coreiface.BlockAPI, opts ...func(cfg *Config)) (*Dsync, error) {
	cfg := &Config{
		PushPreCheck:   DefaultDagPrecheck,
		PushFinalCheck: DefaultDagFinalCheck,
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

		requireAllBlocks: cfg.RequireAllBlocks,
		allowRemoves:     cfg.AllowRemoves,

		preCheck:             cfg.PushPreCheck,
		finalCheck:           cfg.PushFinalCheck,
		onCompleteHook:       cfg.PushComplete,
		getDagInfoCheck:      cfg.GetDagInfoCheck,
		openBlockStreamCheck: cfg.OpenBlockStreamCheck,
		removeCheck:          cfg.RemoveCheck,

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
		m := http.NewServeMux()
		m.Handle("/dsync", HTTPRemoteHandler(ds))

		ds.httpServer = &http.Server{
			Addr:    cfg.HTTPRemoteAddress,
			Handler: m,
		}
	}

	if cfg.Libp2pHost != nil {
		log.Debug("dsync: adding p2p handler")
		ds.p2pHandler = newp2pHandler(ds, cfg.Libp2pHost)
	}

	return ds, nil
}

// ProtocolVersion reports the current procotol version for dsync
func (ds *Dsync) ProtocolVersion() (protocol.ID, error) {
	return DsyncProtocolID, nil
}

// StartRemote makes dsync available for remote requests, starting an HTTP
// server if a listening address is specified.
// StartRemote returns immediately. Stop remote service by cancelling
// the passed-in context.
func (ds *Dsync) StartRemote(ctx context.Context) error {
	if ds.httpServer == nil && ds.p2pHandler == nil {
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

	if ds.p2pHandler != nil {
		log.Debug("dsync: adding dsync protocol to the host")
		ds.p2pHandler.host.SetStreamHandler(DsyncProtocolID, ds.p2pHandler.LibP2PStreamHandler)
	}

	log.Debug("dsync remote started")
	return nil
}

func (ds *Dsync) syncableRemote(remoteAddr string) (rem DagSyncable, err error) {
	// if a valid base58 peerID is passed, we're doing a p2p dsync
	if id, err := peer.IDB58Decode(remoteAddr); err == nil {
		if ds.p2pHandler == nil {
			return nil, fmt.Errorf("no p2p host provided to perform p2p dsync")
		}
		rem = &p2pClient{remotePeerID: id, p2pHandler: ds.p2pHandler}
	} else if strings.HasPrefix(remoteAddr, "http") {
		rem = &HTTPClient{URL: remoteAddr}
	} else {
		return nil, fmt.Errorf("unrecognized push address string: %s", remoteAddr)
	}

	return rem, nil
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
	rem, err := ds.syncableRemote(remoteAddr)
	if err != nil {
		return nil, err
	}
	return NewPush(ds.lng, info, rem, pinOnComplete)
}

// NewPull creates a pull. A pull fetches an entire DAG from a remote, placing
// it in the local block store
func (ds *Dsync) NewPull(cidStr, remoteAddr string, meta map[string]string) (*Pull, error) {
	rem, err := ds.syncableRemote(remoteAddr)
	if err != nil {
		return nil, err
	}
	return NewPull(cidStr, ds.lng, ds.bapi, rem, meta)
}

// NewReceiveSession takes a manifest sent by a remote and initiates a
// transfer session. It returns a manifest/diff of the blocks the reciever needs
// to have a complete DAG new sessions are created with a deadline for completion
func (ds *Dsync) NewReceiveSession(info *dag.Info, pinOnComplete bool, meta map[string]string) (sid string, diff *dag.Manifest, err error) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(ds.sessionTTLDur))

	if err = ds.preCheck(ctx, *info, meta); err != nil {
		cancel()
		return
	}
	log.Debugf("creating receive session for CID: %s with %d meta keys", info.RootCID().String(), len(meta))

	if pinOnComplete && ds.pin == nil {
		err = fmt.Errorf("remote doesn't support pinning")
		cancel()
		return
	}

	sess, err := newSession(ctx, ds.lng, ds.bapi, info, !ds.requireAllBlocks, pinOnComplete, meta)
	if err != nil {
		cancel()
		return
	}

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
			Err:    fmt.Errorf("sid %q not found", sid),
		}
	}

	// ReceiveBlock accepts a block from the sender, placing it in the local blockstore
	res := sess.ReceiveBlock(hash, bytes.NewReader(data))

	// check if transfer has completed, if so finalize it, but only once
	if res.Status == StatusOk && sess.IsFinalizedOnce() {
		if err := ds.finalizeReceive(sess); err != nil {
			return ReceiveResponse{
				Hash:   sess.info.RootCID().String(),
				Status: StatusErrored,
				Err:    err,
			}
		}
	}

	return res
}

// ReceiveBlocks ingests blocks being pushed into the local store
func (ds *Dsync) ReceiveBlocks(ctx context.Context, sid string, r io.Reader) error {
	sess, ok := ds.sessionPool[sid]
	if !ok {
		log.Debugf("couldn't find session. sid=%q", sid)
		return fmt.Errorf("sid %q not found", sid)
	}

	if err := sess.ReceiveBlocks(ctx, r); err != nil {
		log.Debugf("error receiving blocks. err=%q", err)
		return err
	}

	if err := ds.finalizeReceive(sess); err != nil {
		log.Debugf("error finalizing receive. err=%q", err)
		return err
	}

	return nil
}

// TODO (b5): needs to be called if someone tries to sync a DAG that requires
// no blocks for an early termination, ensuring that we cache a dag.Info in
// that case as well
func (ds *Dsync) finalizeReceive(sess *session) error {
	log.Debug("finalizing receive session", sess.id)
	if err := ds.finalCheck(sess.ctx, *sess.info, sess.meta); err != nil {
		log.Error("final check error", err)
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

	if ds.onCompleteHook != nil {
		if err := ds.onCompleteHook(sess.ctx, *sess.info, sess.meta); err != nil {
			log.Errorf("completed hook error: %s", err)
			return err
		}
	}

	return nil
}

// GetDagInfo gets the manifest for a DAG rooted at id, checking any configured cache before falling back to generating a new manifest
func (ds *Dsync) GetDagInfo(ctx context.Context, hash string, meta map[string]string) (info *dag.Info, err error) {
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

	info, err = dag.NewInfo(ctx, ds.lng, id)
	if err != nil {
		return nil, err
	}

	if ds.getDagInfoCheck != nil {
		if err = ds.getDagInfoCheck(ctx, *info, meta); err != nil {
			return nil, err
		}
	}

	return info, nil
}

// GetBlock returns a single block from the store
func (ds *Dsync) GetBlock(ctx context.Context, hash string) ([]byte, error) {
	rdr, err := ds.bapi.Get(ctx, path.New(hash))
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(rdr)
}

// OpenBlockStream creates a block stream of the contents of the dag.Info
func (ds *Dsync) OpenBlockStream(ctx context.Context, info *dag.Info, meta map[string]string) (io.ReadCloser, error) {
	if ds.openBlockStreamCheck != nil {
		if err := ds.openBlockStreamCheck(ctx, *info, meta); err != nil {
			return nil, err
		}
	}

	rdr, err := NewManifestCARReader(ctx, ds.lng, info.Manifest, nil)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(rdr), nil
}

// RemoveCID unpins a CID if removes are enabled, does not immideately remove
// unpinned content
func (ds *Dsync) RemoveCID(ctx context.Context, cidStr string, meta map[string]string) error {
	if !ds.allowRemoves {
		return ErrRemoveNotSupported
	}

	log.Debug("removing cid", cidStr)
	if ds.removeCheck != nil {
		info := dag.Info{Manifest: &dag.Manifest{Nodes: []string{cidStr}}}
		if err := ds.removeCheck(ctx, info, meta); err != nil {
			return err
		}
	}

	if ds.pin != nil {
		return ds.pin.Rm(ctx, path.New(cidStr))
	}

	return nil
}
