package dsync

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	host "github.com/libp2p/go-libp2p-core/host"
	net "github.com/libp2p/go-libp2p-core/network"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/qri-io/dag"
	"github.com/qri-io/dag/dsync/p2putil"
	"github.com/ugorji/go/codec"
)

const (
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
	// mtRemoveCID identifies the "remove_cid" message type
	mtRemoveCID = p2putil.MsgType("remove_cid")
)

type p2pClient struct {
	remotePeerID peer.ID
	*p2pHandler
}

// assert at compile time that p2pClient implements DagSyncable
var _ DagSyncable = (*p2pClient)(nil)

func (c *p2pClient) NewReceiveSession(info *dag.Info, pinOnComplete bool, meta map[string]string) (sid string, diff *dag.Manifest, err error) {
	var data []byte
	if data, err = info.MarshalCBOR(); err != nil {
		return
	}

	headers := []string{"phase", "request", "pin", fmt.Sprintf("%t", pinOnComplete)}
	for key, val := range meta {
		headers = append(headers, key, val)
	}

	msg := p2putil.NewMessage(c.host.ID(), mtNewReceive, data).WithHeaders(headers...)

	log.Debugf("new push session msg to %s", c.remotePeerID)
	res, err := c.sendMessage(context.Background(), msg, c.remotePeerID)
	if err != nil {
		return
	}

	sid = res.Header("sid")
	diff, err = dag.UnmarshalCBORManifest(res.Body)
	log.Debugf("received pin pessage from %s", c.remotePeerID)
	return sid, diff, err
}

// ProtocolVersion indicates the version of dsync the remote speaks, only
// available after a handshake is established
func (c *p2pClient) ProtocolVersion() (protocol.ID, error) {
	if string(c.remoteProtocolID) == "" {
		return "", ErrUnknownProtocolVersion
	}
	return c.remoteProtocolID, nil
}

// ReceiveBlock places a block on the remote
func (c *p2pClient) ReceiveBlock(sid string, id cid.Cid, data []byte) ReceiveResponse {
	msg := p2putil.NewMessage(c.host.ID(), mtReceiveBlock, data).WithHeaders(
		"sid", sid,
		"cid", id.String(),
		"phase", "request",
	)

	res, err := c.sendMessage(context.Background(), msg, c.remotePeerID)
	if err != nil {
		return ReceiveResponse{
			Cid:    id,
			Status: StatusErrored,
			Err:    fmt.Errorf("remote error: %s", err.Error()),
		}
	}

	resCid, err := cid.Parse(res.Header("cid"))
	if err != nil {
		return ReceiveResponse{
			Cid:    id,
			Status: StatusErrored,
			Err:    fmt.Errorf("receive block response didn't include well-formed CID header"),
		}
	}

	rr := ReceiveResponse{
		Cid: resCid,
	}

	if e := res.Header("error"); e != "" {
		rr.Err = fmt.Errorf("%s", e)
	}

	switch res.Header("status") {
	case "ok":
		rr.Status = StatusOk
	case "retry":
		rr.Status = StatusRetry
	default:
		rr.Status = StatusErrored
	}

	return rr
}

// GetDagInfo asks the remote for info specified by a the root identifier
// string of a DAG
func (c *p2pClient) GetDagInfo(ctx context.Context, id cid.Cid, meta map[string]string) (info *dag.Info, err error) {

	headers := []string{"phase", "request", "cid", id.String()}
	for key, val := range meta {
		headers = append(headers, key, val)
	}

	msg := p2putil.NewMessage(c.host.ID(), mtGetDagInfo, nil).WithHeaders(headers...)

	res, err := c.sendMessage(ctx, msg, c.remotePeerID)
	if err != nil {
		return nil, err
	}

	info = &dag.Info{}
	err = codec.NewDecoder(bytes.NewReader(res.Body), &codec.CborHandle{}).Decode(info)
	return info, err
}

// GetBlock gets a block of data from the remote
func (c *p2pClient) GetBlock(ctx context.Context, id cid.Cid) (rawdata []byte, err error) {
	msg := p2putil.NewMessage(c.host.ID(), mtGetBlock, nil).WithHeaders(
		"cid", id.String(),
	)
	res, err := c.sendMessage(ctx, msg, c.remotePeerID)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

// RemoveCID asks the remote to remove a CID
func (c *p2pClient) RemoveCID(ctx context.Context, id cid.Cid, meta map[string]string) (err error) {
	headers := []string{"phase", "request", "cid", id.String()}
	for key, val := range meta {
		headers = append(headers, key, val)
	}

	msg := p2putil.NewMessage(c.host.ID(), mtRemoveCID, nil).WithHeaders(headers...)

	res, err := c.sendMessage(ctx, msg, c.remotePeerID)
	if err != nil {
		return err
	}

	if e := res.Header("error"); e != "" {
		return fmt.Errorf(e)
	}

	return nil
}

// p2pHandler implements dsync as a libp2p protocol handler
type p2pHandler struct {
	dsync            *Dsync
	host             host.Host
	handlers         map[p2putil.MsgType]p2putil.HandlerFunc
	remoteProtocolID protocol.ID
}

// newp2pHandler creates a p2p remote stream handler from a dsync.Remote
func newp2pHandler(dsync *Dsync, host host.Host) *p2pHandler {
	c := &p2pHandler{dsync: dsync, host: host}
	c.handlers = map[p2putil.MsgType]p2putil.HandlerFunc{
		mtNewReceive:   c.HandleNewReceive,
		mtReceiveBlock: c.HandleReceiveBlock,
		mtGetDagInfo:   c.HandleReqManifest,
		mtGetBlock:     c.HandleGetBlock,
		mtRemoveCID:    c.HandleRemoveCID,
	}
	return c
}

// LibP2PStreamHandler provides remote access over p2p
func (c *p2pHandler) LibP2PStreamHandler(s net.Stream) {
	c.handleStream(p2putil.WrapStream(s), nil)
}

// sendMessage opens a stream & sends a message to a peer id
func (c *p2pHandler) sendMessage(ctx context.Context, msg p2putil.Message, pid peer.ID) (p2putil.Message, error) {
	s, err := c.host.NewStream(ctx, pid, DsyncProtocolID)
	if err != nil {
		return p2putil.Message{}, fmt.Errorf("error opening stream: %s", err.Error())
	}
	c.remoteProtocolID = s.Protocol()
	defer s.Close()

	// now that we have a confirmed working connection
	// tag this peer as supporting the qri protocol in the connection manager
	// rem.host.ConnManager().TagPeer(pid, dsyncSupportKey, dsyncSupportValue)

	ws := p2putil.WrapStream(s)
	replies := make(chan p2putil.Message)
	go c.handleStream(ws, replies)
	if err := ws.SendMessage(msg); err != nil {
		return p2putil.Message{}, err
	}

	reply := <-replies
	return reply, nil
}

// handleStream is a loop which receives and handles messages
// When Message.HangUp is true, it exits. This will close the stream
// on one of the sides. The other side's receiveMessage() will error
// with EOF, thus also breaking out from the loop.
func (c *p2pHandler) handleStream(ws *p2putil.WrappedStream, replies chan p2putil.Message) {
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

		if replies != nil {
			go func() { replies <- msg }()
		}

		handler, ok := c.handlers[msg.Type]
		if !ok {
			// log.Errorf("peer %s sent unrecognized message type '%s', hanging up", n.ID, msg.Type)
			break
		}

		if hangup := handler(ws, msg); hangup {
			break
		}
	}

	log.Debugf("hangup: %s", ws.Stream())
	ws.Close()
}

// HandleNewReceive requests a new send session from the remote, which will return
// a delta manifest of blocks the remote needs and a session id that must
// be sent with each block
func (c *p2pHandler) HandleNewReceive(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	if msg.Header("phase") == "request" {
		info, err := dag.UnmarshalCBORDagInfo(msg.Body)
		if err != nil {
			return true
		}

		pinOnComplete := msg.Header("pin") == "true"
		meta := map[string]string{}
		for key, val := range msg.Headers {
			if key != "pin" && key != "phase" {
				meta[key] = val
			}
		}

		sid, diff, err := c.dsync.NewReceiveSession(info, pinOnComplete, meta)
		if err != nil {
			// TODO (b5) - send error response
			// msg =
			fmt.Printf("error creating new receive: %s\n", err.Error())
			return true
		}

		enc, err := diff.MarshalCBOR()
		if err != nil {
			// TODO (b5) - send error response
			// msg =
			fmt.Printf("error marshaling cbor: %s\n", err.Error())
			return true
		}

		res := msg.WithHeaders(
			"phase", "response",
			"sid", sid,
		).Update(enc)

		if err := ws.SendMessage(res); err != nil {
			return true
		}
	}

	// w.Header().Set("sid", sid)
	// json.NewEncoder(w).Encode(diff)
	return false
}

// HandleReceiveBlock places a block on the remote
func (c *p2pHandler) HandleReceiveBlock(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	if msg.Header("phase") == "request" {
		sid := msg.Headers["sid"]
		id, err := cid.Parse(msg.Headers["cid"])
		if err != nil {
			return true
		}
		rr := c.dsync.ReceiveBlock(sid, id, msg.Body)

		var status, errString string
		switch rr.Status {
		case StatusErrored:
			status = "errored"
		case StatusOk:
			status = "ok"
		case StatusRetry:
			status = "retry"
		}

		if rr.Err != nil {
			errString = rr.Err.Error()
		}

		res := msg.WithHeaders(
			"phase", "response",
			"cid", id.String(),
			"status", status,
			"error", errString,
		)

		if err := ws.SendMessage(res); err != nil {
			return true
		}
	}

	return false
}

// HandleReqManifest asks the remote for a manifest specified by the root ID of a DAG
func (c *p2pHandler) HandleReqManifest(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	id, err := cid.Parse(msg.Header("cid"))
	if err != nil {
		return true
	}

	res := msg.WithHeaders("phase", "response")

	meta := map[string]string{}
	for key, val := range msg.Headers {
		if key != "cid" && key != "phase" {
			meta[key] = val
		}
	}

	// TODO (b5): pass a context into here
	if di, err := c.dsync.GetDagInfo(context.Background(), id, meta); err != nil {
		res = res.WithHeaders("error", err.Error())
	} else {
		data, err := di.MarshalCBOR()
		if err != nil {
			return
		}
		res = res.Update(data)
	}

	if err := ws.SendMessage(res); err != nil {
		return true
	}
	return false
}

// HandleGetBlock gets a block from the remote
func (c *p2pHandler) HandleGetBlock(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	id, err := cid.Parse(msg.Header("cid"))
	if err != nil {
		return true
	}

	res := msg.WithHeaders("phase", "response")

	// TODO (b5) - plumb a context in here
	data, err := c.dsync.GetBlock(context.Background(), id)
	if err != nil {
		res = res.WithHeaders("error", err.Error())
	} else {
		res = res.Update(data)
	}

	if err := ws.SendMessage(res); err != nil {
		return true
	}
	return false
}

// HandleRemoveCID removes a CID on the remote
func (c *p2pHandler) HandleRemoveCID(ws *p2putil.WrappedStream, msg p2putil.Message) (hangup bool) {
	if msg.Header("phase") == "request" {

		id, err := cid.Parse(msg.Header("cid"))
		if err != nil {
			return true
		}

		meta := map[string]string{}
		for key, val := range msg.Headers {
			if key != "pin" && key != "phase" {
				meta[key] = val
			}
		}

		res := msg.WithHeaders(
			"phase", "response",
			"cid", id.String(),
		)

		if err := c.dsync.RemoveCID(context.Background(), id, meta); err != nil {
			res = msg.WithHeaders("error", err.Error())
		}

		if err := ws.SendMessage(res); err != nil {
			return true
		}
	}

	return false
}
