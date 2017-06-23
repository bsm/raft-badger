package raftbadger

import (
	"errors"
	"io"
	"os"

	"github.com/dgraph-io/badger/badger"
	"github.com/hashicorp/raft"
)

var ErrKeyNotFound = errors.New("not found")

// LogStore combines raft.LogStore and io.Closer
type LogStore interface {
	raft.LogStore
	io.Closer
}

// NewLogStore uses the supplied options to open a log store. badger.DefaultOptions
// will be used, if nil is passed for opt.
func NewLogStore(dir string, opt *badger.Options) (LogStore, error) { return newStore(dir, opt) }

// StableStore combines raft.StableStore and io.Closer
type StableStore interface {
	raft.StableStore
	io.Closer
}

// NewStableStore uses the supplied options to open a stable store. badger.DefaultOptions
// will be used, if nil is passed for opt.
func NewStableStore(dir string, opt *badger.Options) (StableStore, error) {
	return newStore(dir, opt)
}

// --------------------------------------------------------------------

type store struct {
	kv *badger.KV
}

func newStore(dir string, opt *badger.Options) (*store, error) {
	if opt == nil {
		opt = new(badger.Options)
		*opt = badger.DefaultOptions
	}
	if opt.Dir == "" {
		opt.Dir = dir
	}
	if opt.ValueDir == "" {
		opt.ValueDir = dir
	}

	if err := os.MkdirAll(opt.Dir, 0777); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opt.ValueDir, 0777); err != nil {
		return nil, err
	}

	kv, err := badger.NewKV(opt)
	if err != nil {
		return nil, err
	}
	return &store{
		kv: kv,
	}, nil
}

// Close is used to gracefully close the connection.
func (s *store) Close() error { return s.kv.Close() }

// FirstIndex returns the first known index from the Raft log.
func (s *store) FirstIndex() (uint64, error) { return s.firstIndex(false) }

// LastIndex returns the last known index from the Raft log.
func (s *store) LastIndex() (uint64, error) { return s.firstIndex(true) }

// GetLog is used to retrieve a log from BoltDB at a given index.
func (s *store) GetLog(idx uint64, log *raft.Log) error {
	var item badger.KVItem
	if err := s.kv.Get(uint64ToBytes(idx), &item); err != nil {
		return err
	} else if item.Value() == nil {
		return raft.ErrLogNotFound
	}
	return gobDecode(item.Value(), log)
}

// StoreLog is used to store a single raft log
func (s *store) StoreLog(log *raft.Log) error {
	buf, err := gobEncode(log)
	if err != nil {
		return err
	}
	defer bufPool.Put(buf)

	return s.kv.Set(uint64ToBytes(log.Index), buf.Bytes())
}

// StoreLogs is used to store a set of raft logs
func (s *store) StoreLogs(logs []*raft.Log) error {
	entries := make([]*badger.Entry, 0, len(logs))
	for _, log := range logs {
		buf, err := gobEncode(log)
		if err != nil {
			return err
		}
		defer bufPool.Put(buf)

		key := uint64ToBytes(log.Index)
		entries = badger.EntriesSet(entries, key, buf.Bytes())
	}
	return s.kv.BatchSet(entries)
}

// DeleteRange is used to delete logs within a given range inclusively.
func (s *store) DeleteRange(min, max uint64) error {
	it := s.kv.NewIterator(badger.IteratorOptions{PrefetchSize: 100})
	defer it.Close()

	var entries []*badger.Entry
	for it.Seek(uint64ToBytes(min)); it.Valid(); it.Next() {
		key := it.Item().Key()
		if bytesToUint64(key) > max {
			break
		}
		entries = badger.EntriesDelete(entries, key)
	}
	return s.kv.BatchSet(entries)
}

// Set is used to set a key/value set outside of the raft log
func (s *store) Set(k, v []byte) error {
	return s.kv.Set(k, v)
}

// Get is used to retrieve a value from the k/v store by key
func (s *store) Get(k []byte) ([]byte, error) {
	var item badger.KVItem
	if err := s.kv.Get(k, &item); err != nil {
		return nil, err
	} else if item.Value() == nil {
		return nil, ErrKeyNotFound
	}
	return item.Value(), nil
}

// SetUint64 is like Set, but handles uint64 values
func (s *store) SetUint64(key []byte, val uint64) error {
	return s.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (s *store) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

func (s *store) firstIndex(reverse bool) (uint64, error) {
	it := s.kv.NewIterator(badger.IteratorOptions{
		PrefetchSize: 1,
		FetchValues:  false,
		Reverse:      reverse,
	})
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		return bytesToUint64(it.Item().Key()), nil
	}
	return 0, nil
}
