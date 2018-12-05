package dsync

import (
	"context"
	"testing"

	"github.com/qri-io/dag"
)

func TestFetch(t *testing.T) {
	ctx := context.Background()
	a, b := newLocalRemoteIPFSAPI(ctx, t)
	id := addOneBlockDAG(b, t)

	aGetter := &dag.NodeGetter{Dag: a.Dag()}

	rem := &remote{
		lng:  &dag.NodeGetter{Dag: b.Dag()},
		bapi: b.Block(),
	}

	f, err := NewFetch(ctx, id.String(), aGetter, a.Block(), rem)
	if err != nil {
		t.Fatal(err)
	}

	if err := f.Do(); err != nil {
		t.Fatal(err)
	}

	if _, err = dag.NewManifest(ctx, &dag.NodeGetter{Dag: b.Dag()}, id); err != nil {
		t.Errorf("expected dag to be available in local node after fetch. error: %s", err.Error())
	}
}
