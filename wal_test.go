package lsmtree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewWalManager(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		manager, err := newWalManager(dir+"/wal", 1024*8)
		assert.NoError(t, err)
		assert.NotNil(t, manager)
	})
}

func TestOpenWalSegment(t *testing.T) {
	t.Run("directory doesnt exist", func(t *testing.T) {
		file, err := openWalSegment("tmp", 1, 1024)
		assert.Error(t, err)
		assert.Nil(t, file)
	})

	t.Run("create file", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		file, err := openWalSegment(dir, 1, 1024)
		assert.NoError(t, err)
		assert.NotNil(t, file)
	})
}

func TestWalSegment_Append(t *testing.T) {
	t.Run("synchronous", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		file, err := openWalSegment(dir, 1, 1024)
		assert.NoError(t, err)
		assert.NotNil(t, file)

		err = file.Append(walTransaction{
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
	})
}

func TestWalSegment_Sync(t *testing.T) {
	t.Run("synchronous", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		file, err := openWalSegment(dir, 1, 1024)
		assert.NoError(t, err)
		assert.NotNil(t, file)

		err = file.Append(walTransaction{
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

		err = file.Sync()
		assert.NoError(t, err)
	})
}
