package lsmtree

import (
	"os"
	"path"
)

type (
	walTransactionChangeType byte

	// walManager is a simple wrapper around the entire WAL concept. It manages writes to the WAL
	// files as well as creating new segments. If needed it can also read writes back from a point
	// in time.
	walManager struct {
		// Directory is the folder where WAL files will be stored.
		Directory string

		// currentSegment is the WAL segment that is currently being used for all transactions. As
		// transactions are committed there are appended here. Once this segment reaches a max size
		// then a new segment will be created.
		currentSegment *walSegment
	}

	// walSegment represents a single chunk of the entire WAL. This chunk is limited by file size
	// and will only become larger than that file size if the last change persisted to it pushes it
	// beyond that limit. This is to allow for values that might actually be larger than a single
	// segment would normally allow.
	walSegment struct {
		// SegmentId represents the numeric progression of the WAL. This is an ascending value with
		// the higher values being the most recent set of changes.
		SegmentId uint64

		// File is just an accessor for the actual data on the disk for the WAL segment.
		File ReaderWriterAt
	}

	// walTransaction represents a single batch of changes that must be all committed to the state
	// of the database, or none of them can be committed. The walTransaction should be suffixed with
	// a checksum in the WAL file to make sure that the transaction is not corrupt if it needs to be
	// read back.
	walTransaction struct {
		TransactionId uint64
		Entries       []walTransactionChange
	}

	// walTransactionChange represents a single change made to the database state during a single
	// transaction. It will indicate whether the pair is being set, or whether the key is being
	// deleted from the store. If the key is being deleted then value will be nil and will not be
	// encoded.
	walTransactionChange struct {
		// Type whether the pair is being set or deleted.
		Type walTransactionChangeType

		// Key is the unique identifier for tha pair. This key does not include the transactionId as
		// wal entries do not need to be sorted except by the order the change was committed.
		Key Key

		// Value is the value we want to store in the database. This will be nil if we are deleting
		// a key.
		Value []byte
	}
)

const (
	// walTransactionChangeTypeSet indicates that the value is being set.
	walTransactionChangeTypeSet walTransactionChangeType = iota

	// walTransactionChangeTypeDelete indicates that the value is being deleted.
	walTransactionChangeTypeDelete
)

// openWalSegment will open or create a wal segment file if it does not exist.
func openWalSegment(directory string, segmentId uint64) (*walSegment, error) {
	filePath := path.Join(directory, getWalSegmentFileName(segmentId))

	// We want to be able to read/write the file. If the file does not exist we want to create it.
	flags := os.O_CREATE | os.O_RDWR

	// We are only appending to the file, and we want to be the only process with the file open.
	// This might change later as it might prove to be more efficient to have a single writer and
	// multiple readers for a single file.
	mode := os.ModeAppend | os.ModeExclusive

	file, err := os.OpenFile(filePath, flags, mode)
	if err != nil {
		return nil, err
	}

	return &walSegment{
		SegmentId: segmentId,
		File:      file,
	}, nil
}

// Append adds a transaction entry to the end of the WAL segment.
func (w *walSegment) Append(txn walTransaction) error {
	panic("not implemented")
}

// Sync will flush the changes made to the wal file to the disk if the file interface implements
// the CanSync interface. If it does not then nothing happens and nil is returned.
func (w *walSegment) Sync() error {
	if canSync, ok := w.File.(CanSync); ok {
		return canSync.Sync()
	}

	return nil
}
