package lsmtree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpenWalSegment(t *testing.T) {
	t.Run("directory doesnt exist", func(t *testing.T) {
		file, err := openWalSegment("tmp", 1)
		assert.Error(t, err)
		assert.Nil(t, file)
	})

	t.Run("create file", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		file, err := openWalSegment(dir, 1)
		assert.NoError(t, err)
		assert.NotNil(t, file)
	})
}
