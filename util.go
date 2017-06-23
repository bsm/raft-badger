package raftbadger

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"
)

var bufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

var rdrPool = sync.Pool{
	New: func() interface{} { return new(bytes.Reader) },
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func gobEncode(v interface{}) (*bytes.Buffer, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	if err := gob.NewEncoder(buf).Encode(v); err != nil {
		bufPool.Put(buf)
		return nil, err
	}
	return buf, nil
}

func gobDecode(p []byte, v interface{}) error {
	rdr := rdrPool.Get().(*bytes.Reader)
	rdr.Reset(p)
	defer rdrPool.Put(rdr)

	return gob.NewDecoder(rdr).Decode(v)
}
