package dsync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/qri-io/dag"
)

// DagStreamable is an interface for sending and fetching all blocks in a given
// manifest in one trip
type DagStreamable interface {
	PutBlocks(ctx context.Context, sid string, ng ipld.NodeGetter, mfst *dag.Manifest, progCh chan cid.Cid) error
	FetchBlocks(ctx context.Context, sid string, mfst *dag.Manifest, progCh chan cid.Cid) error
}

func protocolSupportsDagStreaming(pid protocol.ID) bool {
	versions := strings.Split(strings.TrimPrefix(string(pid), "/dsync/"), ".")
	if len(versions) != 3 {
		log.Debugf("unexpected version string in protocol.ID pid=%q versions=%v", pid, versions)
		return false
	}

	major, err := strconv.Atoi(versions[0])
	if err != nil {
		log.Debugf("error parsing major version number in protocol.ID pid=%q versions=%v", pid, versions)
		return false
	}

	minor, err := strconv.Atoi(versions[1])
	if err != nil {
		log.Debugf("error parsing minor version number in protocol.ID pid=%q versions=%v", pid, versions)
		return false
	}

	// anything above 0.2 is considered to support Dag Streaming
	return major >= 0 && minor >= 2
}

// NewManifestCARReader creates a Content-addressed ARchive on the fly from a manifest
// and a node getter. It fetches blocks in order from the list of cids in the
// manifest and writes them to a buffer as the reader is consumed
// The roots specified in the archive header match the manifest RootCID method
// If an incomplete manifest graph is passed to NewManifestCARReader, the resulting
// archive will not be a complete graph. This is permitted by the spec, and
// used by dsync to create an archive of only-missing-blocks
// for more on CAR files, see: https://github.com/ipld/specs/blob/master/block-layer/content-addressable-archives.md
// If supplied a non-nil channel progress channel, the stream will send as
// each CID is buffered to the read stream
func NewManifestCARReader(ctx context.Context, ng ipld.NodeGetter, mfst *dag.Manifest, progCh chan cid.Cid) (io.Reader, error) {

	cids := make([]cid.Cid, 0, len(mfst.Nodes))
	for _, cidStr := range mfst.Nodes {
		id, err := cid.Decode(cidStr)
		if err != nil {
			return nil, err
		}
		id, err = cid.Cast(id.Bytes())
		if err != nil {
			return nil, err
		}
		cids = append(cids, id)
	}

	buf := &bytes.Buffer{}
	header := &car.CarHeader{
		Roots:   []cid.Cid{mfst.RootCID()},
		Version: 1,
	}
	err := car.WriteHeader(header, buf)
	if err != nil {
		return nil, err
	}

	str := &mfstCarReader{
		ctx:    ctx,
		cids:   cids,
		ng:     ng,
		buf:    buf,
		progCh: progCh,
	}
	return str, nil
}

type mfstCarReader struct {
	i      int
	ctx    context.Context
	cids   []cid.Cid
	ng     ipld.NodeGetter
	buf    *bytes.Buffer
	progCh chan cid.Cid
}

func (str *mfstCarReader) Read(p []byte) (int, error) {
	for {
		// check for remaining bytes after last block is read
		if str.i == len(str.cids) && str.buf.Len() > 0 {
			return str.buf.Read(p)
		}

		// break loop on sufficent buffer length
		if str.buf.Len() > len(p) {
			break
		}

		if err := str.readBlock(); err != nil {
			return 0, err
		}
	}

	return io.ReadFull(str.buf, p)
}

// readBlock extends the buffer by one block
func (str *mfstCarReader) readBlock() error {
	if str.i == len(str.cids) {
		return io.EOF
	}
	nd, err := str.ng.Get(str.ctx, str.cids[str.i])
	if err != nil {
		fmt.Printf("error getting block: %s\n", err)
		return err
	}

	str.i++
	if err = carutil.LdWrite(str.buf, nd.Cid().Bytes(), nd.RawData()); err != nil {
		return err
	}

	if str.progCh != nil {
		go func() { str.progCh <- nd.Cid() }()
	}

	return nil
}

// AddAllFromCARReader consumers a CAR reader stream, placing all blocks in the
// given blockstore
func AddAllFromCARReader(ctx context.Context, bapi coreiface.BlockAPI, r io.Reader, progCh chan cid.Cid) (int, error) {
	rdr, err := car.NewCarReader(r)
	if err != nil {
		return 0, err
	}

	added := 0
	buf := &bytes.Buffer{}
	for {
		blk, err := rdr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return added, err
		}

		if _, err := buf.Write(blk.RawData()); err != nil {
			return added, err
		}
		if _, err = bapi.Put(ctx, buf); err != nil {
			return added, err
		}

		buf.Reset()
		added++

		log.Debugf("wrote block %s", blk.Cid())
		if progCh != nil {
			go func() { progCh <- blk.Cid() }()
		}
	}

	return added, nil
}
