package lsmtree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpen(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		options := DefaultOptions()
		options.WALDirectory = dir
		options.DataDirectory = dir

		db, err := Open(options)
		assert.NoError(t, err)
		assert.NotNil(t, db)

		err = db.Close()
		assert.NoError(t, err)
	})
}
