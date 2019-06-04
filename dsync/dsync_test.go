package dsync

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
)

// func ExampleDsync() {
// 	// first some boilerplate setup. In this example we're using "full" IPFS nodes
// 	// but all that's required is a blockstore and dag core api implementation
// 	ctx, done := context.WithCancel(context.Background())
// 	defer done()
// 	localNode, remoteNode := mustNewLocalRemoteIPFSAPI(ctx)
	
// 	// add a single block graph to the local repo, getting back a content identifier
// 	cid := mustAddOneBlockDAG(localNode)
	
// 	// create a local Dsync instance
// 	localDsync := New(localNode.Dag(), localNode.Block())

// 	remoteAddr := ":9595"
// 	// create the remote instance, configuring it to accept DAGs
// 	remoteDsync := New(remoteNode.Dag(), remoteNode.Block(), func(cfg *Config){
// 		cfg.HTTPRemoteAddress = remoteAddr
// 	})

// 	push, err := localDsync.NewPush(cid.String(), remoteAddr, true)
// 	if err != nil {
// 		panic(err)
// 	}

// 	if err := push.Do(ctx); err != nil {
// 		panic(err)
// 	}



// }

func mustNewLocalRemoteIPFSAPI(ctx context.Context) (local, remote coreiface.CoreAPI) {
	_, a, err := makeAPI(ctx)
	if err != nil {
		panic(err)
	}

	_, b, err := makeAPI(ctx)
	if err != nil {
		panic(err)
	}

	return a, b
}

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

func mustAddOneBlockDAG(node coreiface.CoreAPI) cid.Cid {
	ctx := context.Background()
	f := files.NewReaderFile(ioutil.NopCloser(strings.NewReader("y"+strings.Repeat("o", 350))))
	path, err := node.Unixfs().Add(ctx, f)
	if err != nil {
		panic(err)
	}
	return path.Cid()
}


func addOneBlockDAG(node coreiface.CoreAPI, t *testing.T) cid.Cid {
	ctx := context.Background()
	f := files.NewReaderFile(ioutil.NopCloser(strings.NewReader("y"+strings.Repeat("o", 350))))
	path, err := node.Unixfs().Add(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	return path.Cid()
}
