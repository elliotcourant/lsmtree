package lsmtree

import (
	"encoding/binary"
	"github.com/elliotcourant/buffers"
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

		// MaxWALSegmentSize is the largest a segment file is allowed to be grown to excluding the
		// last transaction committed to it. (see Options)
		MaxWALSegmentSize uint64

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

		// Space is used to keep track of where data should be written as well as how much space is
		// left in the file.
		Space freeSpace

		// File is just an accessor for the actual data on the disk for the WAL segment.
		File ReaderWriterAt
	}

	// walTransaction represents a single batch of changes that must be all committed to the state
	// of the database, or none of them can be committed. The walTransaction should be suffixed with
	// a checksum in the WAL file to make sure that the transaction is not corrupt if it needs to be
	// read back.
	walTransaction struct {
		// TransactionId is the "timestamp" of the changes made.
		TransactionId uint64

		// Timestamp is used for MVCC.
		Timestamp uint64

		// HeapId is used to determine where the keys for the batch have been stored. If this value
		// is greater than 0 then the changes have been pushed to the heap file specified. But if
		// the value is 0 then that means the keys have not yet been pushed to the disk.
		HeapId uint64

		// ValueFileId is used to determine where the values for this batch are stored. If this
		// value is greater than 0 then the changes have been pushed to the value file specified. If
		// the value is 0 then that means the values have not yet been flushed to the disk.
		ValueFileId uint64

		// Entries are all of the changes made to the database state during this batch.
		Entries []walTransactionChange
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

// newWalManager will create the WAL manager object.
func newWalManager(directory string, maxWalSegmentSize uint64) (*walManager, error) {
	// Create/verify that the directory exists. If it does not exist then this will create it. If
	// the dir does exist then nothing will happen here.
	if err := newDirectory(directory); err != nil {
		return nil, err
	}

	return &walManager{
		Directory:         directory,
		MaxWALSegmentSize: maxWalSegmentSize,
		currentSegment:    nil,
	}, nil
}

// openWalSegment will open or create a wal segment file if it does not exist.
func openWalSegment(directory string, segmentId uint64, size int32) (*walSegment, error) {
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

	var space freeSpace

	// If the current file size less than or equal to 8 then we know it's a new file and we need to
	// create the freeSpace map. This is because we should be allocating files of a size large
	// enough to contain the map AND the data.
	if stat.Size() <= 8 {
		space = newFreeSpace(size)
	} else {
		spaceBytes := make([]byte, 8)
		if n, err := file.ReadAt(spaceBytes, 0); err != nil {
			return nil, err
		} else if n < 8 {
			return nil, ErrCantReadFreeSpace
		}

		space = newFreeSpaceFromBytes(spaceBytes)
	}

	return &walSegment{
		SegmentId: segmentId,
		Space:     space,
		File:      file,
	}, nil
}

// Append adds a transaction entry to the WAL segment. A transaction header is inserted at the top
// of the file, and the transaction data is added to a buffer from the end of file. If the write is
// successful then no error will be returned. If there is not enough space to write the transaction
// to this WAL segment then ErrInsufficientSpace will be returned.
func (w *walSegment) Append(txn walTransaction) (err error) {
	// The header will always be 16 bytes and consists of a single 64 bit integer and two 32 bit
	// integers.
	header := make([]byte, 16)

	// Encode the transactions changes to be written to the file.
	data := txn.Encode()

	// Allocate space for the item to be written to the WAL.
	ok, headerOffset, dataOffset := w.Space.Allocate(header, data)
	if !ok {
		return ErrInsufficientSpace
	}

	// The header will always be 16 bytes, it will contain the the TransactionId, and the start and
	// end offsets for the actual transaction changes within the file.
	binary.BigEndian.PutUint64(header[0:8], txn.TransactionId)
	binary.BigEndian.PutUint32(header[8:12], uint32(dataOffset))
	binary.BigEndian.PutUint32(header[12:16], uint32(dataOffset+int64(len(data))))

	// Write the header to the file.
	if _, err = w.File.WriteAt(header, headerOffset); err != nil {
		return err
	}

	// Write the actual transaction data.
	if _, err = w.File.WriteAt(data, dataOffset); err != nil {
		return err
	}

	// Everything worked, we can return nil.
	return nil
}

// UpdateTransactionFlush will update the heapId and valueFileId's of the specified transaction
// within the WAL segment. If the transaction could not be found then ok will be false. If the write
// failed then an error will be returned. This will fsync the WAL segment.
func (w *walSegment) UpdateTransactionFlush(transactionId, heapId, valueFileId uint64) (
	ok bool, err error,
) {
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

// Encode returns the binary representation of the walTransaction.
// 1. 8 Bytes: Transaction ID
// 2. 8 Bytes: Timestamp
// 3. 8 Bytes: Heap ID
// 4. 8 Bytes: Value File ID
// 5. 2 Bytes: Number Of Changes
// 6. Repeated: walTransactionChange
func (t *walTransaction) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(t.TransactionId)
	buf.AppendUint64(t.Timestamp)
	buf.AppendUint64(t.HeapId)
	buf.AppendUint64(t.ValueFileId)
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
