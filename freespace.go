package lsmtree

import (
	"encoding/binary"
	"sync/atomic"
)

type (
	freeSpace struct {
		space uint64
	}
)

func newFreeSpace(size int32) freeSpace {
	high, low := int64(8)<<32, int64(size)
	return freeSpace{
		space: uint64(high | low),
	}
}

func (f *freeSpace) Insert(header, data []byte) (ok bool, headerOffset, dataOffset int64) {
	headerSize, dataSize := len(header), -len(data)
	deltaBinary := make([]byte, 8)
	binary.BigEndian.PutUint32(deltaBinary[:4], uint32(headerSize))
	binary.BigEndian.PutUint32(deltaBinary[4:], uint32(dataSize))
	delta := binary.BigEndian.Uint64(deltaBinary) + 1<<32
	result := atomic.AddUint64(&f.space, delta)
	newStart, newEnd := int32(result>>32), int32(result)

	// If we allocated too much then we need to deduct the allocation we just made.
	if newEnd-newStart < 0 {
		atomic.AddUint64(&f.space, -delta)
		return false, 0, 0
	}

	return true, int64(newStart), int64(newEnd)
}

func (f *freeSpace) Current() (headerOffset, dataOffset int64) {
	current := f.space
	start, end := int32(current>>32), int32(current)
	return int64(start), int64(end)
}

func (f *freeSpace) Space() int64 {
	start, end := f.Current()
	return end - start
}
