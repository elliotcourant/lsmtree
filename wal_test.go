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

func TestWalSegment_Append(t *testing.T) {
	t.Run("synchronous", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		file, err := openWalSegment(dir, 1)
		assert.NoError(t, err)
		assert.NotNil(t, file)

		size, err := file.Append(walTransaction{
			TransactionId: 12345,
			Entries: []walTransactionChange{
				{
					Type:  walTransactionChangeTypeSet,
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
				{
					Type:  walTransactionChangeTypeSet,
					Key:   []byte("key2"),
					Value: []byte("value3"),
				},
				{
					Type: walTransactionChangeTypeDelete,
					Key:  []byte("key4"),
				},
			},
		})
		assert.NoError(t, err)
		assert.Greater(t, size, uint64(0))
	})
}

func TestWalSegment_Sync(t *testing.T) {
	t.Run("synchronous", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		file, err := openWalSegment(dir, 1)
		assert.NoError(t, err)
		assert.NotNil(t, file)

		size, err := file.Append(walTransaction{
			TransactionId: 12345,
			Entries: []walTransactionChange{
				{
					Type:  walTransactionChangeTypeSet,
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
				{
					Type:  walTransactionChangeTypeSet,
					Key:   []byte("key2"),
					Value: []byte("value3"),
				},
				{
					Type: walTransactionChangeTypeDelete,
					Key:  []byte("key4"),
				},
			},
		})
		assert.NoError(t, err)
		assert.Greater(t, size, uint64(0))

		err = file.Sync()
		assert.NoError(t, err)
	})
}
