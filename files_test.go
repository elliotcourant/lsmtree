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

func TestGetPathExists(t *testing.T) {
	t.Run("does not exist", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		exists := getPathExists(dir + "/fake")
		assert.False(t, exists)
	})

	t.Run("does not exist", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		exists := getPathExists(dir)
		assert.True(t, exists)
	})
}

func TestCreateDirectory(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		path := dir + "/data"

		exists := getPathExists(path)
		assert.False(t, exists)

		err := createDirectory(path)
		assert.NoError(t, err)

		exists = getPathExists(path)
		assert.True(t, exists)
	})
}

func TestTakeOwnership(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		// TODO (elliotcourant) actually create a failing case for this test.
		err := takeOwnership(dir)
		assert.NoError(t, err)
	})
}

func TestNewDirectory(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		path := dir + "/data"

		exists := getPathExists(path)
		assert.False(t, exists)

		err := newDirectory(path)
		assert.NoError(t, err)

		exists = getPathExists(path)
		assert.True(t, exists)
	})
}
