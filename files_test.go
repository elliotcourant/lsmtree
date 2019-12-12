package lsmtree

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGetValueFileName(t *testing.T) {
	fileIds := []uint64{
		1,
		2,
		132,
		532532,
		899329,
		math.MaxUint64,
	}
	for _, fileId := range fileIds {
		filename := getValueFileName(fileId)
		assert.NotEmpty(t, filename)
		assert.Len(t, filename, 18)
	}
}

func TestGetWalSegmentFileName(t *testing.T) {
	segmentIds := []uint64{
		1,
		2,
		132,
		532532,
		899329,
		math.MaxUint64,
	}
	for _, segmentId := range segmentIds {
		filename := getWalSegmentFileName(segmentId)
		assert.NotEmpty(t, filename)
		assert.Len(t, filename, 18)
	}
}
