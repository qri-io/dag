// Package dsync implements point-to-point block-syncing between a local and remote source
// it's like rsync, but specific to merkle-dags
package dsync

import (
	"context"
	"fmt"
	"io"
	"math/rand"

	"github.com/qri-io/dag"

	"gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
	coreiface "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface"
)

// Response defines the result of sending a block, or attempting to send a block
type Response struct {
	Hash   string
	Status ResponseStatus
	Err    error
}

// ResponseStatus defines types of results for a request
type ResponseStatus int

const (
	// StatusErrored indicates the request failed and cannot be retried
	StatusErrored ResponseStatus = -1
	// StatusOk indicates the request comp
	StatusOk ResponseStatus = 0
	// StatusRetry indicates the request can be attempted again
	StatusRetry ResponseStatus = 1
)

// Remote is an interface for a source that can be synced to
type Remote interface {
	// ReqSession requests a new session from the remote, which will return a
	// delta manifest of blocks the remote needs and a session id that must
	// be sent with each block
	ReqSession(mfst *dag.Manifest) (sid string, diff *dag.Manifest, err error)
	// PutBlock places a block on the remote
	PutBlock(sid, hash string, data []byte) Response
}

// Send coordinates sending a manifest to a receiver, tracking progress and state
type Send struct {
	sid         string          // session ID for this push, generated by receiver
	ctx         context.Context // session context
	mfst        *dag.Manifest   // manifest we're sending
	diff        *dag.Manifest   // returned difference
	lng         ipld.NodeGetter // local NodeGetter (Block Getter)
	remote      Remote          // place we're sending to
	parallelism int             // number of "tracks" for sending along
	prog        dag.Completion  // progress state
	progCh      chan dag.Completion
	blocksCh    chan string
	responses   chan Response
	retries     chan string
}

const (
	// default to parallelism of 4, which roughly matches browsers
	defaultSendParallelism = 4
	// total number of retries to attempt before send is considered faulty
	// TODO (b5): this number should be retries *per object*, and a much lower
	// number, like 5.
	maxRetries = 25
)

// NewSend gets a local path to a remote place using a local NodeGetter and a remote
func NewSend(ctx context.Context, lng ipld.NodeGetter, mfst *dag.Manifest, remote Remote) (*Send, error) {
	parallelism := defaultSendParallelism
	if len(mfst.Nodes) < parallelism {
		parallelism = len(mfst.Nodes)
	}

	ps := &Send{
		ctx:         ctx,
		mfst:        mfst,
		lng:         lng,
		remote:      remote,
		parallelism: parallelism,
		blocksCh:    make(chan string),
		progCh:      make(chan dag.Completion),
		responses:   make(chan Response),
		retries:     make(chan string),
	}
	return ps, nil
}

// Do executes the send, blocking until complete
func (snd *Send) Do() (err error) {
	snd.sid, snd.diff, err = snd.remote.ReqSession(snd.mfst)
	if err != nil {
		return err
	}

	snd.prog = dag.NewCompletion(snd.mfst, snd.diff)
	go snd.completionChanged()

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
			ctx:       snd.ctx,
			blocksCh:  snd.blocksCh,
			responses: snd.responses,
			lng:       snd.lng,
			remote:    snd.remote,
		}
		sends[i].start()
	}

	errCh := make(chan error)

	// receive block responses
	go func(sends []sender, errCh chan error) {
		// handle *all* responses from senders. it's very important that this loop
		// never block, so all responses are handled in their own goroutine
		for res := range snd.responses {
			go func(r Response) {
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
					fmt.Println(r.Err)
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
func (snd *Send) Completion() <-chan dag.Completion {
	return snd.progCh
}

func (snd *Send) completionChanged() {
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
	responses chan Response
	stopCh    chan bool
}

func (s sender) start() {
	go func() {
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
						s.responses <- Response{
							Hash:   hash,
							Status: StatusErrored,
							Err:    err,
						}
					}
					node, err := s.lng.Get(s.ctx, id)
					if err != nil {
						s.responses <- Response{
							Hash:   hash,
							Status: StatusErrored,
							Err:    err,
						}
						return
					}
					s.responses <- s.remote.PutBlock(s.sid, hash, node.RawData())
				}()
			case <-s.stopCh:
				return
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

func (s sender) stop() {
	go func() {
		s.stopCh <- true
	}()
}

// Receive tracks state of receiving a manifest of blocks from a remote
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
