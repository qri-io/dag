package dsync

import (
	"context"
	"io"
	"time"

	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
	coreiface "gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface"
	"gx/ipfs/QmUJYo4etAQqFfSS2rarFAE97eNGB8ej64YkRT2SmsYD4r/go-ipfs/core/coreapi/interface/options"
)

// NewTestReceivers returns a Receivers pointer suitable for testing, and not much else
func NewTestReceivers() *Receivers {
	return &Receivers{
		ctx:     context.Background(),
		lng:     newTestNodeGetter(),
		bapi:    newTestBlockAPI(),
		pool:    make(map[string]*Receive),
		cancels: make(map[string]context.CancelFunc),
		TTLDur:  time.Hour * 5,
	}
}

type testNodeGetter struct{}

func newTestNodeGetter() ipld.NodeGetter {
	return &testNodeGetter{}
}

func (t *testNodeGetter) Get(context.Context, cid.Cid) (ipld.Node, error) {
	return nil, nil
}

func (t *testNodeGetter) GetMany(context.Context, []cid.Cid) <-chan *ipld.NodeOption {
	return nil
}

type testBlockAPI struct{}

func newTestBlockAPI() coreiface.BlockAPI {
	return &testBlockAPI{}
}

func (t *testBlockAPI) Put(context.Context, io.Reader, ...options.BlockPutOption) (coreiface.BlockStat, error) {
	return nil, nil
}

func (t *testBlockAPI) Get(context.Context, coreiface.Path) (io.Reader, error) {
	return nil, nil
}

func (t *testBlockAPI) Rm(context.Context, coreiface.Path, ...options.BlockRmOption) error {
	return nil
}

func (t *testBlockAPI) Stat(context.Context, coreiface.Path) (coreiface.BlockStat, error) {
	return nil, nil
}
