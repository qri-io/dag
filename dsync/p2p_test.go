package dsync

import (
	"context"
	"fmt"
	"testing"

	core "github.com/ipfs/go-ipfs/core"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/qri-io/dag"
)

func TestNewP2P(t *testing.T) {
	// this is the same as ExampleNew, but uses a p2p transport instead of HTTP
	ctx, done := context.WithCancel(context.Background())
	defer done()

	// nodeA is an ipfs instance that will create a DAG
	// nodeB is another ipfs instance nodeA will push that DAG to
	nodeA, nodeB, capiA, capiB := mustNewLocalRemoteIPFSNode(ctx)

	// Local Setup:
	// add a single block graph to nodeA getting back a content identifier
	cid := mustAddOneBlockDAG(capiA)
	// make a localNodeGetter, when performing dsync we don't want to fetch
	// blocks from the dweb
	aLocalDS, err := NewLocalNodeGetter(capiA)
	if err != nil {
		t.Fatal(err) // don't t.Fatal. real programs handle errors.
	}
	// create nodeA's Dsync instance
	aDsync, err := New(aLocalDS, capiA.Block(), OptLibp2pHost(nodeA.PeerHost))
	if err != nil {
		t.Fatal(err)
	}

	// Remote setup:
	// setup the remote we're going to push to, starting by creating a local node
	// getter
	bLocalDS, err := NewLocalNodeGetter(capiB)
	if err != nil {
		t.Fatal(err)
	}

	// we're going set up our remote to push over p2p, get b's multiaddr
	bAddr := nodeB.Identity.String()

	// create the remote instance, configuring it to accept DAGs
	bDsync, err := New(bLocalDS, capiB.Block(), func(cfg *Config) {
		// configure the remote listening address:
		cfg.Libp2pHost = nodeB.PeerHost

		// we MUST override the PreCheck function. In this example we're making sure
		// no one sends us a bad hash:
		cfg.PushPreCheck = func(ctx context.Context, info dag.Info, _ map[string]string) error {
			if info.Manifest.Nodes[0] == "BadHash" {
				return fmt.Errorf("rejected for secret reasons")
			}
			return nil
		}

		// in order for remotes to allow pinning, they must be provided a PinAPI:
		cfg.PinAPI = capiB.Pin()
	})
	if err != nil {
		t.Fatal(err)
	}

	// start listening for remote pushes & pulls. This should be long running,
	// like a server. Cancel the provided context to close
	if err = bDsync.StartRemote(ctx); err != nil {
		t.Fatal(err)
	}

	// Create a Push:
	push, err := aDsync.NewPush(cid.String(), bAddr, true)
	if err != nil {
		t.Fatal(err)
	}

	// We want to see progress, so we spin up a goroutine to listen for updates
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
		t.Fatal(err)
	}
	// at this point we know the update is finished

	// prove the block is now in nodeB:
	_, err = capiB.Block().Get(ctx, path.New(cid.String()))
	if err != nil {
		t.Fatal(err)
	}

	// block until updates has had a chance to print
	<-waitForFmt

	// Output: 0/1 blocks transferred
	// 1/1 blocks transferred
	// done!
}

func mustNewLocalRemoteIPFSNode(ctx context.Context) (ipfsA, ipfsB *core.IpfsNode, capiA, capiB coreapi.CoreAPI) {
	nd, ca, err := makeAPISwarm(ctx, true, 2)
	if err != nil {
		panic(err)
	}

	return nd[0], nd[1], ca[0], ca[1]
}
