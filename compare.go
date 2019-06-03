package dag

import (
	"context"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// Missing returns a manifest describing blocks that are not in this node for a given manifest
func Missing(ctx context.Context, ng ipld.NodeGetter, m *Manifest) (missing *Manifest, err error) {
	var nodes []string

	for _, idstr := range m.Nodes {
		id, err := cid.Parse(idstr)
		if err != nil {
			return nil, err
		}
		if _, err := ng.Get(ctx, id); err == ipld.ErrNotFound {
			nodes = append(nodes, id.String())
		} else if err != nil {
			return nil, err
		}
	}
	return &Manifest{Nodes: nodes}, nil
}
