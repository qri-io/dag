package dsync

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/qri-io/dag"
)

func TestCarStream(t *testing.T) {
	ctx := context.Background()
	_, a, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, b, err := makeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// yooooooooooooooooooooo...
	f := files.NewReaderFile(ioutil.NopCloser(strings.NewReader("y" + strings.Repeat("o", 350000))))
	path, err := a.Unixfs().Add(ctx, f)
	if err != nil {
		t.Fatal(err)
	}

	aGetter := &dag.NodeGetter{Dag: a.Dag()}

	mfst, err := dag.NewManifest(ctx, aGetter, path.Cid())
	if err != nil {
		t.Fatal(err)
	}

	var sentOnChan []cid.Cid
	progCh := make(chan cid.Cid)
	done := make(chan struct{})
	go func() {
		for {
			cid := <-progCh
			sentOnChan = append(sentOnChan, cid)
			if len(sentOnChan) == len(mfst.Nodes) {
				done <- struct{}{}
			}
		}
	}()

	r, err := NewManifestCARReader(ctx, aGetter, mfst, progCh)
	if err != nil {
		t.Fatal(err)
	}

	added, err := AddAllFromCARReader(ctx, b.Block(), r, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(mfst.Nodes) != added {
		t.Errorf("scanned blocks mismatch. wanted: %d got: %d", len(mfst.Nodes), added)
	}

	<-done
	// TODO (b5) - refactore manifest.Nodes to be of type []cid.Cid & compare
	// slices to be exact here
	if len(mfst.Nodes) != len(sentOnChan) {
		t.Errorf("progress cids channel mismatch. wanted: %d got: %d", len(mfst.Nodes), len(sentOnChan))
	}
}

func TestProtocolSupportsDagStreaming(t *testing.T) {
	cases := []struct {
		pid    protocol.ID
		expect bool
	}{
		{"/dsync/0.2.0", true},
		{"/dsync/0.2.1", true},
		{"/dsync/1.28.10", true},

		{"/dsync/0.1.0", false},
		{"/dsync/", false},
		{"/dsync/bad.number.10", false},
		{"/dsync/1.huh?.10", false},
	}

	for _, c := range cases {
		t.Run(string(c.pid), func(t *testing.T) {
			got := protocolSupportsDagStreaming(c.pid)
			if c.expect != got {
				t.Errorf("support for %q mismatch. want: %t got: %t", c.pid, c.expect, got)
			}
		})
	}
}
