package dsync

import (
	"context"
	"io"
	"time"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// NewTestDsync returns a Dsync pointer suitable for testing
func NewTestDsync() *Dsync {
	return &Dsync{
		lng:            newTestNodeGetter(),
		bapi:           newTestBlockAPI(),
		sessionPool:    make(map[string]*Session),
		sessionCancels: make(map[string]context.CancelFunc),
		sessionTTLDur:  time.Hour * 5,
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

func (t *testBlockAPI) Get(context.Context, path.Path) (io.Reader, error) {
	return nil, nil
}

func (t *testBlockAPI) Rm(context.Context, path.Path, ...options.BlockRmOption) error {
	return nil
}

func (t *testBlockAPI) Stat(context.Context, path.Path) (coreiface.BlockStat, error) {
	return nil, nil
}
