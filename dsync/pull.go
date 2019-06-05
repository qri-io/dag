package dsync

import (
	"bytes"
	"context"
	"fmt"

	"github.com/qri-io/dag"

	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
)

// NewPull sets up fetching a DAG at an id from a remote
func NewPull(cidStr string, lng ipld.NodeGetter, bapi coreiface.BlockAPI, rem DagSyncable) (pull *Pull, err error) {
	f := &Pull{
		path:        cidStr,
		lng:         lng,
		bapi:        bapi,
		remote:      rem,
		parallelism: defaultPullParallelism,
		progCh:      make(chan dag.Completion),
		reqCh:       make(chan string),
		resCh:       make(chan blockResponse),
	}

	return f, nil
}

// NewPullWithInfo creates a pull when we already have a dag.Info
func NewPullWithInfo(info *dag.Info, lng ipld.NodeGetter, bapi coreiface.BlockAPI, rem DagSyncable) (pull *Pull, err error) {
	f, err := NewPull(info.RootCID().String(), lng, bapi, rem)
	if err != nil {
		return nil, err
	}
	f.info = info
	return f, nil
}

// Pull coordinates the transfer of missing blocks in a DAG from a remote to a block store
type Pull struct {
	path        string
	info        *dag.Info
	diff        *dag.Manifest
	remote      DagSyncable
	lng         ipld.NodeGetter
	bapi        coreiface.BlockAPI
	parallelism int
	prog        dag.Completion
	progCh      chan dag.Completion
	reqCh       chan string
	resCh       chan blockResponse
}

// blockResponse is a response from a pull request
type blockResponse struct {
	Hash  string
	Raw   []byte
	Error error
}

// Do executes the pull, blocking until complete
func (f *Pull) Do(ctx context.Context) (err error) {
	// How pulling works:
	// * request a dag.Info from the remote node
	// * determines the progress already made by checking for local blocks
	//   from the manifest
	// * begin to pull the blocks in parallel:
	//    - create a number of pullers
	//    - each puller listens for incoming ids on the request channel
	//      they request the blocks of these hash from the remote & send the
	//      responses to the response channel
	//    - listen for pull responses. responses have two possible outcomes:
	//      - valid hash response: put the incoming block into our local store
	//      - error: send the error over the error channel & bail
	//    - every time we receive a block, check if we're done
	if f.info == nil {
		// request a manifest from the remote if we don't have one
		if f.info, err = f.remote.GetDagInfo(ctx, f.path); err != nil {
			return
		}
	}

	f.diff, err = dag.Missing(ctx, f.lng, f.info.Manifest)
	if err != nil {
		return
	}

	f.prog = dag.NewCompletion(f.info.Manifest, f.diff)
	go f.completionChanged()

	if f.prog.Complete() {
		return nil
	}

	return f.do(ctx)
}

func (f *Pull) do(ctx context.Context) error {
	if len(f.diff.Nodes) < f.parallelism {
		f.parallelism = len(f.diff.Nodes)
	}

	// create pullers
	pullers := make([]puller, f.parallelism)
	for i := 0; i < f.parallelism; i++ {
		pullers[i] = puller{
			id:     i,
			ctx:    ctx,
			remote: f.remote,
			reqCh:  f.reqCh,
			resCh:  f.resCh,
			stopCh: make(chan bool),
		}
		go pullers[i].start()
	}
	defer func() {
		for _, fr := range pullers {
			fr.stop()
		}
	}()

	errCh := make(chan error)
	go func() {
		for {
			select {
			case res := <-f.resCh:
				go func(res blockResponse) {
					if res.Error != nil {
						errCh <- res.Error
						return
					}

					bs, err := f.bapi.Put(ctx, bytes.NewReader(res.Raw))
					if err != nil {
						errCh <- res.Error
					}

					if bs.Path().Cid().String() != res.Hash {
						errCh <- fmt.Errorf("hash integrity mismatch. expected %s, got: %s", bs.Path().Cid().String(), res.Hash)
					}

					// this is the only place we should modify progress after creation
					for i, hash := range f.info.Manifest.Nodes {
						if res.Hash == hash {
							f.prog[i] = 100
						}
					}
					go f.completionChanged()
					if f.prog.Complete() {
						errCh <- nil
						return
					}
				}(res)
			case <-ctx.Done():
				errCh <- nil
			}
		}
	}()

	// fill requests channel with missing ids
	go func() {
		for _, hash := range f.diff.Nodes {
			f.reqCh <- hash
		}
	}()

	return <-errCh
}

// Updates returns a read-only channel of pull completion changes
func (f *Pull) Updates() <-chan dag.Completion {
	return f.progCh
}

func (f *Pull) completionChanged() {
	f.progCh <- f.prog
}

// puller is a parallelizable, stateless struct that pulls blocks
type puller struct {
	id     int
	remote DagSyncable
	ctx    context.Context
	reqCh  <-chan string
	resCh  chan blockResponse
	stopCh chan bool
}

// start has the puller listen for ids coming into the request channel
// it then get's a block from the remote, and passes the response to the
// response channel
// If we get a call on the stop channel, we end the process.
func (f puller) start() {
	for {
		select {
		case hash := <-f.reqCh:
			go func() {
				data, err := f.remote.GetBlock(f.ctx, hash)
				f.resCh <- blockResponse{
					Hash:  hash,
					Raw:   data,
					Error: err,
				}
			}()
		case <-f.stopCh:
			return
		}
	}
}

func (f puller) stop() {
	f.stopCh <- true
}
