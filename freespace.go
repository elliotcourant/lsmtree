package lsmtree

import (
	"encoding/binary"
	"sync/atomic"
)

type (
	// freeSpace is an 8 byte type used to keep track of the are of a file that can still be written
	// to. It will keep track of where a header can be written and where a value can be written.
	// This can only be used in fixed size files. That is; files that will never grow larger than
	// the initial size specified for the freeSpace. The first 4 bytes store the header offset and
	// the last 4 bytes store the value offset. It is stored entirely as a uint64 to allow for
	// atomic changes to it. This is done using bit magic where we can atomically add a delta to the
	// two 32 bit integers in a single addition.
	freeSpace uint64
)

// newFreeSpace will create a new freeSpace map object. It will allocate 8 bytes from the size
// specified to make sure there is enough room for the freeSpace header itself.
func newFreeSpace(size int32) freeSpace {
	high, low := int64(8)<<32, int64(size)
	return freeSpace(high | low)
}

// newFreeSpaceFromBytes will return the freeSpace map from the first 8 bytes of the provided byte
// array.
func newFreeSpaceFromBytes(data []byte) freeSpace {
	return freeSpace(binary.BigEndian.Uint64(data))
}

// Allocate will allocate space within the freeSpace to store the header and the data byte arrays.
// If there is not enough space available then ok will return false. If there is space available for
// the header and the data then ok will be true and the headerOffset will be the index within the
// file where the header should be written to, and the dataOffset will be the index within the file
// where the data can be written.
func (f *freeSpace) Allocate(header, data []byte) (ok bool, headerOffset, dataOffset int64) {
	headerSize, dataSize := len(header), -len(data)
	delta := uint64(headerSize)<<32 | (uint64(dataSize) & 0xffffffff) - 1<<32
	result := atomic.AddUint64((*uint64)(f), delta)
	newStart, newEnd := int32(result>>32), int32(result)

	// If we allocated too much then we need to deduct the allocation we just made.
	if newEnd-newStart < 0 {
		atomic.AddUint64((*uint64)(f), -delta)
		return false, 0, 0
	}

	return true, int64(newStart) - int64(headerSize), int64(newEnd)
}

// Current will return the current headerOffset and dataOffset for the freeSpace. This should NOT be
// used for writing to a file. It does not reflect a thread safe representation of the free space
// within the file.
func (f *freeSpace) Current() (headerOffset, dataOffset int64) {
	current := (uint64)(*f)
	start, end := int32(current>>32), int32(current)
	return int64(start), int64(end)
}

// Space will return the number of bytes available in the file that can be used to store data. This
// can be used to occasionally check how much space is available in the file. This is not a thread
// safe operation. If writes are occurring at the same time that this was checked then it's possible
// for the value returned to be incorrect. This should be checked when no writes are being sent to
// the file.
func (f *freeSpace) Space() int64 {
	start, end := f.Current()
	return end - start
}

// Encode will return the 8 byte representation of the freeSpace map. This should be the first 8
// bytes in a file.
func (f *freeSpace) Encode() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, (uint64)(*f))
	return b
}
