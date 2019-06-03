package dag

import (
	"context"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// NodeGetter wraps the go-ipfs DagAPI to satistfy the IPLD NodeGetter interface
type NodeGetter struct {
	Dag ipld.DAGService
}

// NewNodeGetter returns a new NodeGetter from an IPFS core API
func NewNodeGetter(dsvc ipld.DAGService) *NodeGetter {
	return &NodeGetter{Dag: dsvc}
}

// Get retrieves nodes by CID. Depending on the NodeGetter
// implementation, this may involve fetching the Node from a remote
// machine; consider setting a deadline in the context.
func (ng *NodeGetter) Get(ctx context.Context, id cid.Cid) (ipld.Node, error) {
	return ng.Dag.Get(ctx, id)
}

// GetMany returns a channel of NodeOptions given a set of CIDs.
func (ng *NodeGetter) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption)
	go func() {
		for _, id := range cids {
			n, err := ng.Get(ctx, id)
			ch <- &ipld.NodeOption{Err: err, Node: n}
		}
	}()
	return ch
}
