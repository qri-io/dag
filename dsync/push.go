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

// String returns a string representation of the status
func (s ReceiveResponseStatus) String() string {
	switch s {
	case StatusErrored:
		return "errored"
	case StatusOk:
		return "ok"
	case StatusRetry:
		return "retry"
	}
	return "unknown"
}

const (
	// StatusErrored indicates the request failed and cannot be retried
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
	pinOnComplete bool              // weather dag should be pinned on completion
	sid           string            // session ID for this push, generated by remote
	info          *dag.Info         // info we're sending
	diff          *dag.Manifest     // returned difference
	lng           ipld.NodeGetter   // local NodeGetter (Block Getter)
	remote        DagSyncable       // place we're sending to
	meta          map[string]string // metadata to associate with this push
	parallelism   int               // number of "tracks" for sending along
	prog          dag.Completion    // progress state
	progCh        chan dag.Completion
	blocksCh      chan string
	responses     chan ReceiveResponse
	retries       chan string
}

// NewPush initiates a send for a DAG at an id from a local to a remote.
// Push is initiated by the local node
func NewPush(lng ipld.NodeGetter, info *dag.Info, remote DagSyncable, pinOnComplete bool) (*Push, error) {
	parallelism := defaultPushParallelism
	if len(info.Manifest.Nodes) < parallelism {
		parallelism = len(info.Manifest.Nodes)
	}

	ps := &Push{
		pinOnComplete: pinOnComplete,
		info:          info,
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

// SetMeta associates metadata with a push before its sent. These details may
// be leveraged by applications built on top of dsync. They're ignored by dsync.
// Meta must be set before starting the push
func (snd *Push) SetMeta(meta map[string]string) {
	snd.meta = meta
}

// Do executes the push, blocking until complete
func (snd *Push) Do(ctx context.Context) (err error) {
	log.Debugf("initiating push")
	// how this process works:
	// * Do sends a dag.Info to the remote node
	// * The remote returns a session id for the push, and manifest of blocks to send
	//   this manifest may only ask for blocks it doesn't have by sending a smaller manifest
	// * Do to sends blocks in parallel:
	//   - create a number of senders
	//   - each sender listens on the blocks channel for data to send to the remote
	//   - listen for block responses from the remote, there are three possible
	//     responses
	//      - okay: update the progress
	//      - error: send the error over the errCh
	//      - retry: push the hash to the list of hashes to retry
	//
	// posible TODO (ramfox): it would be great if the fetch and send Do functions
	// followed the same pattern. Specifically the go function that is used to listen for
	// responses
	snd.sid, snd.diff, err = snd.remote.NewReceiveSession(snd.info, snd.pinOnComplete, snd.meta)
	if err != nil {
		log.Debugf("error creating receive session: %s", err)
		return err
	}
	log.Debugf("push has receive session: %s", snd.sid)
	return snd.do(ctx)
}

func (snd *Push) do(ctx context.Context) (err error) {
	snd.prog = dag.NewCompletion(snd.info.Manifest, snd.diff)
	go snd.completionChanged()

	// response said we have nothing to send. all done
	if len(snd.diff.Nodes) == 0 {
		return nil
	}

	protoID, err := snd.remote.ProtocolVersion()
	if err != nil {
		return err
	}

	if protocolSupportsDagStreaming(protoID) {
		if str, ok := snd.remote.(DagStreamable); ok {
			progCh := make(chan cid.Cid)

			go func() {
				for id := range progCh {
					// this is the only place we should modify progress after creation
					idStr := id.String()
					log.Debugf("sent block %s", idStr)
					for i, hash := range snd.info.Manifest.Nodes {
						if idStr == hash {
							snd.prog[i] = 100
						}
					}
					go snd.completionChanged()
				}
			}()

			r, err := NewManifestCARReader(ctx, snd.lng, snd.diff, progCh)
			if err != nil {
				log.Debugf("err creating CARReader err=%q ", err)
				return err
			}

			return str.ReceiveBlocks(ctx, snd.sid, r)
		}
	}

	// create senders
	sends := make([]sender, snd.parallelism)
	for i := 0; i < snd.parallelism; i++ {
		sends[i] = sender{
			id:        i,
			sid:       snd.sid,
			blocksCh:  snd.blocksCh,
			responses: snd.responses,
			lng:       snd.lng,
			remote:    snd.remote,
			stopCh:    make(chan bool),
		}
		go sends[i].start(ctx)
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
					for i, hash := range snd.info.Manifest.Nodes {
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
					log.Debugf("error pushing block. hash=%q error=%q", r.Hash, r.Err)
					errCh <- r.Err
					for _, s := range sends {
						s.stop()
					}
				case StatusRetry:
					log.Debugf("retrying push block. hash=%q error=%q", r.Hash, r.Err)
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

// Updates returns a read-only channel of Completion objects that depict
// transfer state
func (snd *Push) Updates() <-chan dag.Completion {
	return snd.progCh
}

func (snd *Push) completionChanged() {
	snd.progCh <- snd.prog
}

// sender is a parallelizable, stateless struct that sends blocks
type sender struct {
	id        int
	sid       string
	lng       ipld.NodeGetter
	remote    DagSyncable
	blocksCh  chan string
	responses chan ReceiveResponse
	stopCh    chan bool
}

func (s sender) start(ctx context.Context) {
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
					log.Debugf("error parsing sent block: %s", err)
					s.responses <- ReceiveResponse{
						Hash:   hash,
						Status: StatusErrored,
						Err:    err,
					}
				}
				node, err := s.lng.Get(ctx, id)
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
		case <-ctx.Done():
			log.Debugf("sender context done. err=%q", ctx.Err())
			return
		}
	}
}

func (s sender) stop() {
	go func() {
		s.stopCh <- true
	}()
}
