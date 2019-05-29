package dsync

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/qri-io/dag"
)

// ReceiveResponseStatus defines types of results for a request
type ReceiveResponseStatus int

const (
	// StatusErrored igondicates the request failed and cannot be retried
	StatusErrored ReceiveResponseStatus = -1
	// StatusOk indicates the request completed successfully
	StatusOk ReceiveResponseStatus = 0
	// StatusRetry indicates the request can be attempted again
	StatusRetry ReceiveResponseStatus = 1
)

// ReceiveResponse defines the result of sending a block, or attempting to send a block.
type ReceiveResponse struct {
	Hash   string
	Status ReceiveResponseStatus
	Err    error
}

// Push coordinates sending a manifest to a remote, tracking progress and state
type Push struct {
	pinOnComplete bool            // weather dag should be pinned on completion
	sid           string          // session ID for this push, generated by remote
	mfst          *dag.Manifest   // manifest we're sending
	diff          *dag.Manifest   // returned difference
	lng           ipld.NodeGetter // local NodeGetter (Block Getter)
	remote        Remote          // place we're sending to
	parallelism   int             // number of "tracks" for sending along
	prog          dag.Completion  // progress state
	progCh        chan dag.Completion
	blocksCh      chan string
	responses     chan ReceiveResponse
	retries       chan string
}

// NewPush initiates a send for a DAG at an id from a local to a remote.
// Push is initiated by the local node
func NewPush(lng ipld.NodeGetter, mfst *dag.Manifest, remote Remote, pinOnComplete bool) (*Push, error) {
	parallelism := defaultPushParallelism
	if len(mfst.Nodes) < parallelism {
		parallelism = len(mfst.Nodes)
	}

	ps := &Push{
		pinOnComplete: pinOnComplete,
		mfst:          mfst,
		lng:           lng,
		remote:        remote,
		parallelism:   parallelism,
		blocksCh:      make(chan string),
		progCh:        make(chan dag.Completion),
		responses:     make(chan ReceiveResponse),
		retries:       make(chan string),
	}
	return ps, nil
}

// Do executes the send, blocking until complete
func (snd *Push) Do(ctx context.Context) (err error) {
	// First Do sends a manifest to the remote node
	// The remote returns the id of the process, and a diff of the manifest
	// which describes which blocks we need to send
	// We begin to send the blocks in parallel:
	//   - We create a number of senders
	//   - these senders listen on the blocks channel for which blocks
	//     we need to send to the remote
	//   - we create an error channel, sending over this channel will
	//     end the whole process
	//   - we set up a loop that listens for responses from the remote
	//      - if the status is okay, update the progress
	//      - if the status is error, send the error over the errCh
	//      - if the status is retry, push the hash to the list of hashes to retry
	//   - we set up a loop that listens for retries and sends the hash
	//     to retry to the block channel, unless we have reached the maximum
	//     amount of retries
	//   - we set up a loop to push hashes onto the block channel
	//
	// posible TODO (ramfox): it would be great if the fetch and send Do functions
	// followed the same pattern. Specifically the go function that is used to listen for
	// responses
	snd.sid, snd.diff, err = snd.remote.NewReceiveSession(snd.mfst, snd.pinOnComplete)
	if err != nil {
		return err
	}
	return snd.doActualPush(ctx)
}

// // Push executes the send, given that we already have a session and diff
// func (snd *Push) Push(sid string, mfst, diff *dag.Manifest) error {
// 	snd.sid = sid
// 	snd.mfst = mfst
// 	snd.diff = diff
// 	return snd.doActualPush()
// }

func (snd *Push) doActualPush(ctx context.Context) (err error) {
	snd.prog = dag.NewCompletion(snd.mfst, snd.diff)
	go snd.completionChanged()
	// defer close(snd.progCh)

	// response said we have nothing to send. all done
	if len(snd.diff.Nodes) == 0 {
		return nil
	}

	// create senders
	sends := make([]sender, snd.parallelism)
	for i := 0; i < snd.parallelism; i++ {
		sends[i] = sender{
			id:        i,
			sid:       snd.sid,
			ctx:       ctx,
			blocksCh:  snd.blocksCh,
			responses: snd.responses,
			lng:       snd.lng,
			remote:    snd.remote,
			stopCh:    make(chan bool),
		}
		go sends[i].start()
	}

	errCh := make(chan error)

	// receive block responses
	go func(sends []sender, errCh chan error) {
		// handle *all* responses from senders. it's very important that this loop
		// never block, so all responses are handled in their own goroutine
		for res := range snd.responses {
			go func(r ReceiveResponse) {
				switch r.Status {
				case StatusOk:
					// this is the only place we should modify progress after creation
					for i, hash := range snd.mfst.Nodes {
						if r.Hash == hash {
							snd.prog[i] = 100
						}
					}
					go snd.completionChanged()
					if snd.prog.Complete() {
						errCh <- nil
						return
					}
				case StatusErrored:
					errCh <- r.Err
					for _, s := range sends {
						s.stop()
					}
				case StatusRetry:
					snd.retries <- r.Hash
				}
			}(res)
		}
	}(sends, errCh)

	go func(errCh chan error) {
		retries := 0
		for hash := range snd.retries {
			retries++
			if retries == maxRetries {
				for _, s := range sends {
					s.stop()
				}
				errCh <- fmt.Errorf("max %d retries reached", retries)
				return
			}
			snd.blocksCh <- hash
		}
	}(errCh)

	// fill queue with missing blocks to kick off the send
	go func() {
		for _, hash := range snd.diff.Nodes {
			snd.blocksCh <- hash
		}
	}()

	// block until send on errCh
	return <-errCh
}

// Completion returns a read-only channel of updates to completion
func (snd *Push) Completion() <-chan dag.Completion {
	return snd.progCh
}

func (snd *Push) completionChanged() {
	snd.progCh <- snd.prog
}

// sender is a parallelizable, stateless struct that sends blocks
type sender struct {
	id        int
	sid       string
	ctx       context.Context
	lng       ipld.NodeGetter
	remote    Remote
	blocksCh  chan string
	responses chan ReceiveResponse
	stopCh    chan bool
}

func (s sender) start() {
	for {
		select {
		case hash := <-s.blocksCh:
			// here we're syncronizing multiple channels in a select, and in this case
			// we're (probably) firing off a blocking call to s.remote.PutBlock that's
			// waiting on a network response. This can prevent reading on stopCh & ctx.Done
			// which is very bad, so we fire a goroutine to prevent the select loop from
			// ever blocking. Concurrency is fun!
			go func() {
				id, err := cid.Parse(hash)
				if err != nil {
					s.responses <- ReceiveResponse{
						Hash:   hash,
						Status: StatusErrored,
						Err:    err,
					}
				}
				node, err := s.lng.Get(s.ctx, id)
				if err != nil {
					s.responses <- ReceiveResponse{
						Hash:   hash,
						Status: StatusErrored,
						Err:    err,
					}
					return
				}
				s.responses <- s.remote.ReceiveBlock(s.sid, hash, node.RawData())
			}()
		case <-s.stopCh:
			return
		case <-s.ctx.Done():
			return
		}
	}
}

func (s sender) stop() {
	go func() {
		s.stopCh <- true
	}()
}
