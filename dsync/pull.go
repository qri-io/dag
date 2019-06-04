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
func NewPull(cidStr string, lng ipld.NodeGetter, bapi coreiface.BlockAPI, rem Remote) (pull *Pull, err error) {
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
func NewPullWithInfo(info *dag.Info, lng ipld.NodeGetter, bapi coreiface.BlockAPI, rem Remote) (pull *Pull, err error) {
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
	remote      Remote
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
	// First Do requests a manifest from the remote node
	// It determines the progress already made
	// It begins to pull the blocks in parallel:
	// 		- we create a number of pullers
	//    - these pullers listen for incoming ids on the request channel
	//      they request the blocks of these hash from the remote & send the responses
	//      to the response channel
	//    - we create an error channel, sending anything on this channel triggers an end
	//      to the while process
	//    - we then create loop that listens on the response channel for
	//      pull responses:
	//      - if there is a valid response, we put the incoming block into our local store
	//      - if there is an error response, we send the error over the error channel
	//      - if we have finished pulling all the blocks, we send nil over the error channel
	//      - if at anytime we get a timeout aka an alert from context.Done(), we
	//        also send over the error response
	//    - we set up a loop that actually fills the request
	//      channel with ids we want the puller to pull
	//    - These ids are read by the pullers in parallel, they send the requests the
	//      to the remote
	//
	// so three main things:
	//   1) set up the process for pulling the blocks from the remote
	//   2) set up the process for handling the responses from the remote
	//   		(putting the blocks into the local store, erroring, triggering
	//       a completion when all blocks have been pulled, or triggering a
	//       timeout)
	//   3) set up the process for telling the pullers which blocks to
	//      request
	if f.info == nil {
		// request a manifest from the remote if we don't have one
		if f.info, err = f.remote.GetDagInfo(ctx, f.path); err != nil {
			return
		}
	}

	// TODO (ramfox): Right now, Missing uses the nodegetter method Get
	// if you are online, this method attempts to the get the blocks off
	// the network, not only locally. This takes a long time and makes
	// Missing unusable as of right now. Instead, we are passing
	// NewCompletion an empty diff Manifest. We will be asking the remote
	// source for the entire list of blocks, and although in certain cases
	// this may be redundant, it is ultimately faster until we can change Missing
	// f.diff, err = dag.Missing(f.ctx, f.lng, f.mfst)
	// if err != nil {
	// 	return
	// }
	f.diff = &dag.Manifest{Nodes: f.info.Manifest.Nodes}
	f.prog = dag.NewCompletion(f.info.Manifest, f.diff)
	go f.completionChanged()
	// defer close(f.progCh)

	if f.prog.Complete() {
		return nil
	}

	// TODO (b5): this is really terrible to print here, but is *very* helpful info on the CLI
	// we should pipe a completion channel up to the CLI & remove this
	fmt.Printf("   pulling %d blocks\n", len(f.diff.Nodes))

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

// Completion returns a read-only channel of updates to completion
func (f *Pull) Completion() <-chan dag.Completion {
	return f.progCh
}

func (f *Pull) completionChanged() {
	f.progCh <- f.prog
}

// puller is a parallelizable, stateless struct that pulles blocks
type puller struct {
	id     int
	remote Remote
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
