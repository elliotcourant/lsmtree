package lsmtree

import (
	"encoding/binary"
	"github.com/elliotcourant/buffers"
	"os"
	"path"
	"sync/atomic"
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

		// Offset represents the index that should be used for the next write to the WAL file.
		// It is incremented using atomic operations to support concurrent writes.
		Offset uint64

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

	// If we somehow cannot read the stat for the file then something is very wrong. We need to do
	// this because we need to know what offset to start with when appending to the file.
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &walSegment{
		SegmentId: segmentId,
		Offset:    uint64(stat.Size()),
		File:      file,
	}, nil
}

// Append adds a transaction entry to the end of the WAL segment. It will return the new size of the
// current WAL segment to be used to determine if a new segment should be created.
func (w *walSegment) Append(txn walTransaction) (totalSize uint64, err error) {
	// Build the binary entry. Prefix it with a length that includes the 4 bytes for the length
	// itself.
	buf := append(make([]byte, 4), txn.Encode()...)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)))

	// We now know how big the WAL entry will be on disk.
	size := uint64(len(buf))

	// We can then increment the WAL offset for the size of the entry, and then subtract the size to
	// get the offset that we need to insert this entry to.
	offset := atomic.AddUint64(&w.Offset, size) - size

	// Write the data to the file. We don't need to have any logic here, we can simply return the
	// err right away.
	_, err = w.File.WriteAt(buf, int64(offset))

	return offset + size, err
}

// Sync will flush the changes made to the wal file to the disk if the file interface implements
// the CanSync interface. If it does not then nothing happens and nil is returned.
func (w *walSegment) Sync() error {
	if canSync, ok := w.File.(CanSync); ok {
		return canSync.Sync()
	}

	return nil
}

// Encode returns the binary representation of the walTransaction.
// 1. 8 Bytes: Transaction ID
// 2. 2 Bytes: Number Of Changes
// 3. Repeated: walTransactionChange
func (t *walTransaction) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(t.TransactionId)
	buf.AppendUint16(uint16(len(t.Entries)))
	for _, change := range t.Entries {
		buf.Append(change.Encode()...)
	}

	return buf.Bytes()
}

// Encode returns the binary representation of the walTransactionChange.
// 1. 1 Byte: Change Type
// 2. 4+ Bytes: Key
// 3. 0-4+ Bytes: Value (If we are deleting then this is not included.
func (c *walTransactionChange) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(byte(c.Type))
	buf.Append(c.Key...)

	switch c.Type {
	// Right now only a set type will need the actual value. There might
	// be others in the future that do or do not need the value stored.
	case walTransactionChangeTypeSet:
		buf.Append(c.Value...)
	}

	return buf.Bytes()
}
