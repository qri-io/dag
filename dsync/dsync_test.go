package dsync

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/qri-io/dag"
)

func ExampleNew() {
	// first some boilerplate setup. In this example we're using "full" IPFS nodes
	// but all that's required is a blockstore and dag core api implementation
	// in this example we're going to use a single context, which doesn't make
	// much sense in production. At a minimum the contexts for nodeA & nodeB
	// would be separate
	ctx, done := context.WithCancel(context.Background())
	defer done()

	// nodeA is an ipfs instance that will create a DAG
	// nodeB is another ipfs instance nodeA will push that DAG to
	nodeA, nodeB := mustNewLocalRemoteIPFSAPI(ctx)

	// Local Setup:
	// add a single block graph to nodeA getting back a content identifier
	cid := mustAddOneBlockDAG(nodeA)

	// make a localNodeGetter, when performing dsync we don't want to fetch
	// blocks from the dweb
	aLocalDS, err := NewLocalNodeGetter(nodeA)
	if err != nil {
		panic(err) // don't panic. real programs handle errors.
	}

	// create nodeA's Dsync instance
	aDsync, err := New(aLocalDS, nodeA.Block())
	if err != nil {
		panic(err)
	}

	// Remote setup:
	// setup the remote we're going to push to, starting by creating a local node
	// getter
	bng, err := NewLocalNodeGetter(nodeB)
	if err != nil {
		panic(err)
	}

	// we're going set up our remote to push over HTTP
	bAddr := ":9595"

	// create the remote instance, configuring it to accept DAGs
	bDsync, err := New(bng, nodeB.Block(), func(cfg *Config) {
		// configure the remote listening address:
		cfg.HTTPRemoteAddress = bAddr

		// we MUST override the PreCheck function. In this example we're making sure
		// no one sends us a bad hash:
		cfg.PushPreCheck = func(ctx context.Context, info dag.Info, _ map[string]string) error {
			if info.Manifest.Nodes[0].String() == "BadHash" {
				return fmt.Errorf("rejected for secret reasons")
			}
			return nil
		}

		// in order for remotes to allow pinning, they must be provided a PinAPI:
		cfg.PinAPI = nodeB.Pin()
	})
	if err != nil {
		panic(err)
	}

	// start listening for remote pushes & pulls. This should be long running,
	// like a server. Cancel the provided context to close
	if err = bDsync.StartRemote(ctx); err != nil {
		panic(err)
	}

	// Create a Push:
	push, err := aDsync.NewPush(cid, fmt.Sprintf("http://localhost%s/dsync", bAddr), true)
	if err != nil {
		panic(err)
	}

	// We want to see progress, so we spin up a goroutine to listen for  updates
	waitForFmt := make(chan struct{})
	go func() {
		updates := push.Updates()
		for {
			select {
			case update := <-updates:
				fmt.Printf("%d/%d blocks transferred\n", update.CompletedBlocks(), len(update))
				if update.Complete() {
					fmt.Println("done!")
					waitForFmt <- struct{}{}
				}

			case <-ctx.Done():
				// don't leak goroutines
				waitForFmt <- struct{}{}
				return
			}
		}
	}()

	// Do the push
	if err := push.Do(ctx); err != nil {
		panic(err)
	}
	// at this point we know the update is finished

	// prove the block is now in nodeB:
	_, err = nodeB.Block().Get(ctx, path.New(cid.String()))
	if err != nil {
		panic(err)
	}

	// block until updates has had a chance to print
	<-waitForFmt

	// Output: 0/1 blocks transferred
	// 1/1 blocks transferred
	// done!
}

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
	f := files.NewReaderFile(ioutil.NopCloser(strings.NewReader("y" + strings.Repeat("o", 350))))
	path, err := node.Unixfs().Add(ctx, f)
	if err != nil {
		panic(err)
	}
	return path.Cid()
}

func addOneBlockDAG(node coreiface.CoreAPI, t *testing.T) cid.Cid {
	ctx := context.Background()
	f := files.NewReaderFile(ioutil.NopCloser(strings.NewReader("y" + strings.Repeat("o", 350))))
	path, err := node.Unixfs().Add(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	return path.Cid()
}
