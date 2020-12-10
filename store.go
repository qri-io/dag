package dag

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
)

// InfoStore is the interface for a key-value store DAG information, keyed by id strings
// by convention the id stored should match the root id of the DAG being described
type InfoStore interface {
	// store an Info struct at the key, overwriting any previous entry.
	// The most Common way to use this is keying by the RootCID. eg:
	// store.PutDAGInfo(ctx, di.RootCID().String(), di)
	PutDAGInfo(ctx context.Context, key cid.Cid, di *Info) error
	// get dag information stored at key, return ErrInfoNotFound when
	// a key isn't present in the store
	DAGInfo(ctx context.Context, key cid.Cid) (di *Info, err error)
	// Remove an info stored at key. Deletes for keys that don't exist should not
	// return an error, deleted returns true if a value was present before the call
	DeleteDAGInfo(ctx context.Context, key cid.Cid) (deleted bool, err error)
}

// ErrInfoNotFound should be returned by all implementations of DAGInfoStore
// when a DAG isn't found
var ErrInfoNotFound = fmt.Errorf("Info: not found")

// MemInfoStore is an implementation of InfoStore that uses an in-memory map
type MemInfoStore struct {
	lock  sync.Mutex
	infos map[cid.Cid]*Info
}

// NewMemInfoStore creates an in-memory InfoStore
func NewMemInfoStore() InfoStore {
	return &MemInfoStore{
		infos: map[cid.Cid]*Info{},
	}
}

// PutDAGInfo stores an Info struct at the key id, overwriting any previous entry
func (s *MemInfoStore) PutDAGInfo(_ context.Context, id cid.Cid, di *Info) error {
	s.lock.Lock()
	s.infos[id] = di
	s.lock.Unlock()
	return nil
}

// DAGInfo gets dag information stored at key, return ErrInfoNotFound when
// a key isn't present in the store
func (s *MemInfoStore) DAGInfo(_ context.Context, id cid.Cid) (di *Info, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	info, ok := s.infos[id]
	if !ok {
		return nil, ErrInfoNotFound
	}
	return info, nil
}

// DeleteDAGInfo removes the info at key from the store
func (s *MemInfoStore) DeleteDAGInfo(_ context.Context, id cid.Cid) (deleted bool, err error) {
	s.lock.Lock()
	_, deleted = s.infos[id]
	delete(s.infos, id)
	s.lock.Unlock()

	return
}
