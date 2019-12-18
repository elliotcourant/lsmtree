package lsmtree

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestCombinedDelta(t *testing.T) {
	t.Run("bits", func(t *testing.T) {
		initialStart, initialEnd := 8, 1024
		initialBinary := make([]byte, 8)
		binary.BigEndian.PutUint32(initialBinary[:4], uint32(initialStart))
		binary.BigEndian.PutUint32(initialBinary[4:], uint32(initialEnd))
		offset := binary.BigEndian.Uint64(initialBinary)

		bitOffset := uint64(initialStart<<32 | initialEnd)
		assert.Equal(t, offset, bitOffset)
	})

	t.Run("freespace", func(t *testing.T) {
		space := newFreeSpace(64)
		start, end := space.Current()
		fmt.Println(start, end)
		fmt.Println("Space:", space.Space())

		ok, headerOffset, dataOffset := space.Insert([]byte("test"), []byte("test"))
		fmt.Println(ok, headerOffset, dataOffset)

		start, end = space.Current()
		fmt.Println(start, end)
		fmt.Println("Space:", space.Space())

		ok, headerOffset, dataOffset = space.Insert([]byte("test1"), []byte("test"))
		fmt.Println(ok, headerOffset, dataOffset)

		start, end = space.Current()
		fmt.Println(start, end)
		fmt.Println("Space:", space.Space())

		ok, headerOffset, dataOffset = space.Insert([]byte("test1"), []byte("test"))
		fmt.Println(ok, headerOffset, dataOffset)

		start, end = space.Current()
		fmt.Println(start, end)
		fmt.Println("Space:", space.Space())
	})

	t.Run("super broken out", func(t *testing.T) {
		initialStart, initialEnd := 8, 64
		startDelta, endDelta := +8, -12

		initialBinary := make([]byte, 8)
		binary.BigEndian.PutUint32(initialBinary[:4], uint32(initialStart))
		binary.BigEndian.PutUint32(initialBinary[4:], uint32(initialEnd))
		offset := binary.BigEndian.Uint64(initialBinary)
		fmt.Println(offset)
		fmt.Println(initialBinary)

		offsetCopy := offset

		deltaBinary := make([]byte, 8)
		binary.BigEndian.PutUint32(deltaBinary[:4], uint32(startDelta))
		binary.BigEndian.PutUint32(deltaBinary[4:], uint32(endDelta)+math.MaxInt32)
		delta := binary.BigEndian.Uint64(deltaBinary)
		fmt.Println(delta)
		fmt.Println(deltaBinary)

		offsetCopy += delta
		fmt.Println(offsetCopy)
		resultingOffset := make([]byte, 8)
		binary.BigEndian.PutUint64(resultingOffset, offsetCopy)
		fmt.Println(resultingOffset)

		resultingStart := binary.BigEndian.Uint32(resultingOffset[:4])
		resultingEnd := binary.BigEndian.Uint32(resultingOffset[4:])
		fmt.Println(resultingStart, resultingEnd-math.MaxInt32)
		assert.Equal(t, int32(initialStart+startDelta), int32(resultingStart), "start offset did not match")
		assert.Equal(t, int32(initialEnd+endDelta), int32(resultingEnd)-math.MaxInt32, "end offset did not match")
	})

}
