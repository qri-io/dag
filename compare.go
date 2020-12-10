package dag

import (
	"context"
	"errors"
	"strings"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// Missing returns a manifest describing blocks that are not in this node for a given manifest
func Missing(ctx context.Context, ng ipld.NodeGetter, m *Manifest) (missing *Manifest, err error) {
	var nodes CidList

	for _, idstr := range m.Nodes {
		id, err := cid.Parse(idstr)
		if err != nil {
			return nil, err
		}

		_, err = ng.Get(ctx, id)
		if errors.Is(err, ipld.ErrNotFound) || (err != nil && strings.Contains(err.Error(), "not found")) {
			nodes = append(nodes, id)
		} else if err != nil {
			return nil, err
		}
	}
	return &Manifest{Nodes: nodes}, nil
}
