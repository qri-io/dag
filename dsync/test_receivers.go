package dsync

import (
	"context"
	"io"
	"time"

	cid "github.com/ipfs/go-cid"
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
	"github.com/ipfs/go-ipfs/core/coreapi/interface/options"
	ipld "github.com/ipfs/go-ipld-format"
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
