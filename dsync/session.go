package dsync

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/qri-io/dag"
)

// session tracks the state of a transfer
type session struct {
	ctx  context.Context
	lng  ipld.NodeGetter
	bapi coreiface.BlockAPI

	id     string
	pin    bool
	meta   map[string]string
	info   *dag.Info
	diff   *dag.Manifest
	prog   dag.Completion
	progCh chan dag.Completion
	lock   sync.Mutex
	fin    bool
}

// newSession creates a receive state machine
func newSession(ctx context.Context, lng ipld.NodeGetter, bapi coreiface.BlockAPI, info *dag.Info, calcBlockDiff, pinOnComplete bool, meta map[string]string) (s *session, err error) {
	diff := info.Manifest

	if calcBlockDiff {
		log.Debug("calculating block diff")
		if diff, err = dag.Missing(ctx, lng, info.Manifest); err != nil {
			log.Debugf("error calculating diff err=%q", err)
			return nil, err
		}
	}

	s = &session{
		id:     randStringBytesMask(10),
		ctx:    ctx,
		lng:    lng,
		bapi:   bapi,
		info:   info,
		diff:   diff,
		pin:    pinOnComplete,
		meta:   meta,
		prog:   dag.NewCompletion(info.Manifest, diff),
		progCh: make(chan dag.Completion),
	}

	go s.completionChanged()

	log.Debugf("created session: %s", s.id)
	return s, nil
}

// ReceiveBlock accepts a block from the sender, placing it in the local blockstore
func (s *session) ReceiveBlock(hash string, data io.Reader) ReceiveResponse {
	bstat, err := s.bapi.Put(s.ctx, data)

	if err != nil {
		return ReceiveResponse{
			Hash:   hash,
			Status: StatusRetry,
			Err:    err,
		}
	}

	id := bstat.Path().Cid()
	if id.String() != hash {
		return ReceiveResponse{
			Hash:   hash,
			Status: StatusErrored,
			Err:    fmt.Errorf("hash mismatch. expected: '%s', got: '%s'", hash, id.String()),
		}
	}

	// this should be the only place that modifies progress
	for i, h := range s.info.Manifest.Nodes {
		if hash == h {
			s.prog[i] = 100
		}
	}
	go s.completionChanged()

	return ReceiveResponse{
		Hash:   hash,
		Status: StatusOk,
	}
}

func (s *session) ReceiveBlocks(ctx context.Context, r io.Reader) error {
	progCh := make(chan cid.Cid)

	go func() {
		for id := range progCh {
			idStr := id.String()
			for i, h := range s.info.Manifest.Nodes {
				if idStr == h {
					s.prog[i] = 100
				}
			}
			go s.completionChanged()
		}
	}()

	_, err := AddAllFromCARReader(ctx, s.bapi, r, progCh)
	return err
}

// Complete returns if this receive session is finished or not
func (s *session) Complete() bool {
	return s.prog.Complete()
}

func (s *session) completionChanged() {
	s.progCh <- s.prog
}

// IsFinalizedOnce will return true if the session is complete, but only the first time it is
// called, even if multiple threads call this function at the same time
func (s *session) IsFinalizedOnce() bool {
	if !s.Complete() {
		return false
	}
	ret := false
	s.lock.Lock()
	if !s.fin {
		ret = true
		s.fin = true
	}
	defer s.lock.Unlock()
	return ret
}

// the best stack overflow answer evaarrr: https://stackoverflow.com/a/22892986/9416066
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randStringBytesMask(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// type blockApiHaser struct {
// 	bapi coreiface.BlockAPI
// }

// func (bh blockApiHaser) Has(id cid.Cid) (bool, error) {
// 	st, err := bh.bapi.Stat(context.Background(), path.IpfsPath(id))
// 	if errors.Is() {

// 	}
// }

// func NewBlockAPIHaser(bapi coreiface.BlockAPI) dag.BlockHaser {

// }
