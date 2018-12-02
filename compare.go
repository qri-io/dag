package dag

import (
	"context"

	"gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
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
