package dsync

import (
	"bytes"
	"context"
	"fmt"

	"github.com/qri-io/dag"

	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
	coreiface "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface"
)

// NewFetch initiates a fetch for a DAG at id from a remote
func NewFetch(ctx context.Context, id string, lng ipld.NodeGetter, bapi coreiface.BlockAPI, rem Remote) (fetch *Fetch, err error) {
	f := &Fetch{
		ctx:         ctx,
		path:        id,
		lng:         lng,
		bapi:        bapi,
		remote:      rem,
		parallelism: defaultFetchParallelism,
		progCh:      make(chan dag.Completion),
		reqCh:       make(chan string),
		resCh:       make(chan FetchRes),
	}

	return f, nil
}

// NewFetchWithManifest creates a fetch when we already have a manifest
func NewFetchWithManifest(ctx context.Context, mfst *dag.Manifest, lng ipld.NodeGetter, bapi coreiface.BlockAPI, rem Remote) (fetch *Fetch, err error) {
	f, err := NewFetch(ctx, mfst.RootCID().String(), lng, bapi, rem)
	if err != nil {
		return nil, err
	}
	f.mfst = mfst
	return f, nil
}

// Fetch is a request to download a DAG from a remote
type Fetch struct {
	path        string
	mfst        *dag.Manifest
	diff        *dag.Manifest
	ctx         context.Context
	remote      Remote
	lng         ipld.NodeGetter
	bapi        coreiface.BlockAPI
	parallelism int
	prog        dag.Completion
	progCh      chan dag.Completion
	reqCh       chan string
	resCh       chan FetchRes
}

// FetchRes is a response from a fetch request
type FetchRes struct {
	Hash  string
	Raw   []byte
	Error error
}

// Do executes the fetch
func (f *Fetch) Do() (err error) {
	if f.mfst == nil {
		// request a manifest from the remote if we don't have one
		if f.mfst, err = f.remote.ReqManifest(f.ctx, f.path); err != nil {
			return
		}
	}

	f.diff, err = dag.Missing(f.ctx, f.lng, f.mfst)
	if err != nil {
		return
	}

	f.prog = dag.NewCompletion(f.mfst, f.diff)
	go f.completionChanged()
	// defer close(f.progCh)

	if f.prog.Complete() {
		return nil
	}

	// TODO (b5): this is really terrible to print here, but is *very* helpful info on the CLI
	// we should pipe a completion channel up to the CLI & remove this
	fmt.Printf("   fetching %d blocks\n", len(f.diff.Nodes))

	if len(f.diff.Nodes) < f.parallelism {
		f.parallelism = len(f.diff.Nodes)
	}

	// create fetchers
	fetchers := make([]fetcher, f.parallelism)
	for i := 0; i < f.parallelism; i++ {
		fetchers[i] = fetcher{
			id:     i,
			ctx:    f.ctx,
			remote: f.remote,
			reqCh:  f.reqCh,
			resCh:  f.resCh,
			stopCh: make(chan bool),
		}
		go fetchers[i].start()
	}
	defer func() {
		for _, fr := range fetchers {
			fr.stop()
		}
	}()

	errCh := make(chan error)
	go func() {
		for {
			select {
			case res := <-f.resCh:
				go func(res FetchRes) {
					if res.Error != nil {
						errCh <- res.Error
						return
					}

					bs, err := f.bapi.Put(f.ctx, bytes.NewReader(res.Raw))
					if err != nil {
						errCh <- res.Error
					}

					if bs.Path().Cid().String() != res.Hash {
						errCh <- fmt.Errorf("hash integrity mismatch. expected %s, got: %s", bs.Path().Cid().String(), res.Hash)
					}

					// this is the only place we should modify progress after creation
					for i, hash := range f.mfst.Nodes {
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
			case <-f.ctx.Done():
				errCh <- nil
			}
		}
	}()

	// fill requests channel with missing hashes
	go func() {
		for _, hash := range f.diff.Nodes {
			f.reqCh <- hash
		}
	}()

	return <-errCh
}

func (f *Fetch) completionChanged() {
	f.progCh <- f.prog
}

type fetcher struct {
	id     int
	remote Remote
	ctx    context.Context
	reqCh  <-chan string
	resCh  chan FetchRes
	stopCh chan bool
}

func (f fetcher) start() {
	for {
		select {
		case hash := <-f.reqCh:
			go func() {
				data, err := f.remote.GetBlock(f.ctx, hash)
				f.resCh <- FetchRes{
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

func (f fetcher) stop() {
	f.stopCh <- true
}
