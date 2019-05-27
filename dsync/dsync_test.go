package dsync

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
)

func newLocalRemoteIPFSAPI(ctx context.Context, t *testing.T) (local, remote coreiface.CoreAPI) {
	_, a, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, b, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return a, b
}

func addOneBlockDAG(node coreiface.CoreAPI, t *testing.T) cid.Cid {
	ctx := context.Background()
	f := files.NewReaderFile("oh_hey", "oh_hey", ioutil.NopCloser(strings.NewReader("y"+strings.Repeat("o", 350))), nil)
	path, err := node.Unixfs().Add(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	return path.Cid()
}
