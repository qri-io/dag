package dsync

import (
	"bytes"
	"context"
	"fmt"

	host "github.com/libp2p/go-libp2p-host"
	net "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
	"github.com/qri-io/dag"
	"github.com/qri-io/dag/dsync/p2putil"
	"github.com/ugorji/go/codec"
)

const (
	// DsyncProtocolID is the dsyc p2p Protocol Identifier
	DsyncProtocolID = protocol.ID("/dsync")
	// DsyncServiceTag tags the type & version of the dsync service
	DsyncServiceTag = "dsync/0.1.1-dev"
	// default value to give qri peer connections in connmanager, one hunnit
	dsyncSupportValue = 100
)

var (
	// mtNewReceive identifies the "new_receive" message type
	mtNewReceive = p2putil.MsgType("new_receive")
	// mtReceiveBlock asks a remote to accept a block
	mtReceiveBlock = p2putil.MsgType("receive_block")
	// mtGetDagInfo identifies the "get_daginfo" message type
	mtGetDagInfo = p2putil.MsgType("get_daginfo")
	// mtGetBlock identifies the "get_block" message type
	mtGetBlock = p2putil.MsgType("get_block")
)

// p2pDsync implements dsync as a libp2p protocol handler
type p2pDsync struct {
	remotePeerID peer.ID
	host         host.Host
	dsync        *Dsync
	handlers     map[p2putil.MsgType]p2putil.HandlerFunc
}

// newp2pDsync creates a p2p remote stream handler from a dsync.Remote
func newp2pDsync(dsync *Dsync, host host.Host) *p2pDsync {
	c := &p2pDsync{dsync: dsync}
	c.handlers = map[p2putil.MsgType]p2putil.HandlerFunc{
		mtNewReceive: c.HandleReqSend,
		MtPutBlock:   c.HandleReceiveBlock,
		mtGetDagInfo: c.HandleReqManifest,
		mtGetBlock:   c.HandleGetBlock,
	}
	return c
}

// assert at compile time that p2pDsync implements DagSyncable
var _ DagSyncable = (*p2pDsync)(nil)

// NewReceiveSession requests a new push session from the remote, which will return a
// delta manifest of blocks the remote needs and a session id that must
// be sent with each block
func (c *p2pDsync) NewReceiveSession(info *dag.Info, pinOnComplete bool) (sid string, diff *dag.Manifest, err error) {
	var data []byte
	if data, err = info.MarshalCBOR(); err != nil {
		return
	}

	msg := p2putil.NewMessage(c.host.ID(), mtNewReceive, data).WithHeaders(
		"pin", fmt.Sprintf("%t", pinOnComplete),
	)

	res, err := c.sendMessage(context.Background(), msg, c.remotePeerID)
	if err != nil {
		return
	}

	sid = res.Header("sid")
	diff, err = dag.UnmarshalCBORManifest(res.Body)
	return sid, diff, err
}

// ReceiveBlock places a block on the remote
func (c *p2pDsync) ReceiveBlock(sid, hash string, data []byte) ReceiveResponse {
	msg := p2putil.NewMessage(c.host.ID(), mtReceiveBlock, data).WithHeaders(
		"sid", sid,
		"hash", hash,
	)

	res, err := c.sendMessage(context.Background(), msg, c.remotePeerID)
	if err != nil {
		return ReceiveResponse{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("remote error: %s", err.Error()),
		}
	}

	rr := ReceiveResponse{}
	err = codec.NewDecoder(bytes.NewReader(res.Body), &codec.CborHandle{}).Decode(&rr)
	if err != nil {
		return ReceiveResponse{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("decoding response: %s", err.Error),
		}
	}

	return rr
}

// GetDagInfo asks the remote for info specified by a the root identifier
// string of a DAG
func (c *p2pDsync) GetDagInfo(ctx context.Context, cidStr string) (info *dag.Info, err error) {
	msg := p2putil.NewMessage(c.host.ID(), mtGetDagInfo, nil).WithHeaders(
		"manifest", cidStr,
	)

	res, err := c.sendMessage(ctx, msg, c.remotePeerID)
	if err != nil {
		return nil, err
	}

	info = &dag.Info{}
	err = codec.NewDecoder(bytes.NewReader(res.Body), &codec.CborHandle{}).Decode(info)
	return info, err
}

// GetBlock gets a block of data from the remote
func (c *p2pDsync) GetBlock(ctx context.Context, cidStr string) (rawdata []byte, err error) {
	msg := p2putil.NewMessage(c.host.ID(), mtGetBlock, nil).WithHeaders(
		"block", cidStr,
	)
	res, err := c.sendMessage(ctx, msg, c.remotePeerID)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

// sendMessage opens a stream & sends a message to a peer id
func (c *p2pDsync) sendMessage(ctx context.Context, msg p2putil.Message, pid peer.ID) (p2putil.Message, error) {

	s, err := c.host.NewStream(ctx, pid, DsyncProtocolID)
	if err != nil {
		return p2putil.Message{}, fmt.Errorf("error opening stream: %s", err.Error())
	}
	defer s.Close()

	// 	// now that we have a confirmed working connection
	// 	// tag this peer as supporting the qri protocol in the connection manager
	// 	// rem.host.ConnManager().TagPeer(pid, dsyncSupportKey, dsyncSupportValue)

	ws := p2putil.WrapStream(s)
	replies := make(chan p2putil.Message)
	go c.handleStream(ws, replies)
	if err := ws.SendMessage(msg); err != nil {
		return p2putil.Message{}, err
	}

	reply := <-replies
	return reply, nil
}

// LibP2PStreamHandler provides remote access over p2p
func (c *p2pDsync) LibP2PStreamHandler(s net.Stream) {
	c.handleStream(p2putil.WrapStream(s), nil)
}

// handleStream is a loop which receives and handles messages
// When Message.HangUp is true, it exits. This will close the stream
// on one of the sides. The other side's receiveMessage() will error
// with EOF, thus also breaking out from the loop.
func (c *p2pDsync) handleStream(ws *p2putil.WrappedStream, replies chan p2putil.Message) {
	for {
		// Loop forever, receiving messages until the other end hangs up
		// or something goes wrong
		msg, err := ws.ReceiveMessage()

		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			// log.Debugf("error receiving message: %s", err.Error())
			break
		}

		handler, ok := c.handlers[msg.Type]
		if !ok {
			// log.Infof("peer %s sent unrecognized message type '%s', hanging up", n.ID, msg.Type)
			break
		}

		if hangup := handler(ws, msg); hangup {
			break
		}
	}

	ws.Close()
}

// HandleReqSend requests a new send session from the remote, which will return
// a delta manifest of blocks the remote needs and a session id that must
// be sent with each block
func (c *p2pDsync) HandleReqSend(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	info, err := dag.UnmarshalCBORDagInfo(msg.Body)
	if err != nil {
		return true
	}

	pinOnComplete := msg.Header("pin") == "true"
	sid, diff, err := c.dsync.NewReceiveSession(info, pinOnComplete)
	if err != nil {
		// TODO (b5) - send error response
		// msg =
	}

	// msg := msg.WithHeaders(
	// 	"phase" : "response",
	// 	"sid" : sid,
	// ).Update()
	// sid, diff, err := c.remote.ReqSend(mfst)
	// if err != nil {
	// 	return true
	// }

	// w.Header().Set("sid", sid)
	// json.NewEncoder(w).Encode(diff)
	return false
}

// MtPutBlock identifies the "put_block" message type
var MtPutBlock = p2putil.MsgType("put_block")

// HandleReceiveBlock places a block on the remote
func (c *p2pDsync) HandleReceiveBlock(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	sid := msg.Headers["sid"]
	hash := msg.Headers["hash"]
	res := c.dsync.ReceiveBlock(sid, hash, msg.Body)

	if res.Status == StatusErrored {
		// w.WriteHeader(http.StatusInternalServerError)
		// w.Write([]byte(res.Err.Error()))
	} else if res.Status == StatusRetry {
		// w.WriteHeader(http.StatusBadRequest)
		// w.Write([]byte(res.Err.Error()))
	} else {
		// w.WriteHeader(http.StatusOK)
	}

	return false
}

// HandleReqManifest asks the remote for a manifest specified by the root ID of a DAG
func (c *p2pDsync) HandleReqManifest(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	// rh.remote.ReqManifest()
	return false
}

// HandleGetBlock gets a block from the remote
func (c *p2pDsync) HandleGetBlock(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	return false
}
