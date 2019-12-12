package lsmtree

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

var (
	// Make sure that the os.File struct implements the writer and reader at interfaces.
	_ ReaderWriterAt = &os.File{}
)

var (
	// ErrBadValueChecksum is returned when a value is read from the value file, but the checksum
	// stored with the value does not match the calculated checksum of the value read. This is used
	// as an indicator of file corruption.
	ErrBadValueChecksum = errors.New("bad value checksum")

	// ErrBrokenValue is returned when the entire value could not be read from from the value file.
	// Or when the entire value could not be written to the file.
	ErrIncompleteValue = errors.New("incomplete value")

	// ErrCreatingChecksum is returned when a value is being written to the value file but the
	// checksum could not be created.
	ErrCreatingChecksum = errors.New("could not create checksum for value")
)

type (
	// valueManager wraps all of the value files and manages reads and writes of actual values.
	valueManager struct {
		// directory is the folder where all valueFiles will be stored.
		directory string

		// writeLocks are acquired while a readLock is still held. The read lock is then released.
		// This ensures that two threads cannot try to write to the files map at the same time.
		writeLock sync.Mutex

		// readLock is to make sure that reads are not attempted while a change is being made to the
		// file map. To modify the files map, a readLock and writeLock must be held.
		readLock sync.RWMutex

		// files is just a map of all of the valueFiles in memory by their fileId.
		files map[uint64]*valueFile
	}

	// valueFile represents an append only file that is used to store actual values for the
	// database. Each file is a chunk of the total database file and contains an array of twe types
	// of data. The value itself (which is stored as a raw byte array) and a checksum for the value.
	valueFile struct {
		// FileId represents a unique identifier for this file. The id is globally unique and will
		// not collide with any other value files.
		FileId uint64

		// Offset is used to keep track of the last index in the file that was written, each time a
		// new value is written the offset is incremented before the value is actually written, this
		// is to allocate more space for the file but also to allocate space for the value being
		// written early. Because the offset is incremented atomically multiple writes can occur at
		// the same time without conflicting. Each write will write to it's specific allocation in
		// the file.
		Offset uint64

		// File is a simple Writer and Reader At interface to support very fast random reads and
		// fast concurrent writes. Right now this is an os.File but this could be replaced if it
		// ever needed to be.
		File ReaderWriterAt
	}
)

// openValueFile will open a value file with the Id specified. If the file does not exist it will
// create the file. The file is opened with the append, create and read/write flags, and the append
// and exclusive mode.
func openValueFile(directory string, fileId uint64) (*valueFile, error) {
	// Get an actual file path for the directory and the fileId specified.
	filePath := path.Join(directory, getValueFileName(fileId))

	// We want to be able to read/write the file. If the file does not exist we want to create it.
	flags := os.O_CREATE | os.O_RDWR

	// We are only appending to the file, and we want to be the only process with the file open.
	// This might change later as it might prove to be more efficient to have a single writer and
	// multiple readers for a single file.
	mode := os.ModeAppend | os.ModeExclusive

	// Open/create the file with the flags and mode specified.
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

	f := &valueFile{
		FileId: fileId,
		Offset: uint64(stat.Size()),
		File:   file,
	}

	return f, nil
}

// Read will return the byte array for a value at the address provided. Values are suffixed with a
// 32-bit checksum when they are written. If the checksum does not match when the value is read then
// an ErrBadValueChecksum will be returned here. This is to prevent unintentionally using a value
// that is corrupt. If the entire value cannot be read then an ErrIncompleteValue is returned.
// To recover the value for either of these failures, the WAL entry for this item should be found
// and replayed.
func (f *valueFile) Read(offset, size uint64) ([]byte, error) {
	// We need an extra 4 bytes for the checksum
	value := make([]byte, size+4)

	// Read the value into the buffer at the specified offset.
	// If there is a problem just return early.
	if n, err := f.File.ReadAt(value, int64(offset)); err != nil {
		return nil, err
	} else if n != len(value) {
		// If we didn't get an error but the number of bytes read does not match the number of bytes
		// that we were looking for then we need to return an error.
		return nil, ErrIncompleteValue
	}

	// Validate the checksum.
	{
		h := fnv.New32()

		// If we fail to write the checksum from the value or if the entire value could not be
		// written to the hash then we want to fail here and assume the checksum is bad.
		if n, err := h.Write(value[:size]); err != nil || uint64(n) != size {
			return nil, ErrBadValueChecksum
		}

		// actualChecksum is the hash of the value we read from the file.
		actualChecksum := h.Sum32()

		// readChecksum is the hash of the value that was stored in the file.
		readChecksum := binary.BigEndian.Uint32(value[size:])

		// If the checksums to not match then that means the checksum in the file is wrong, or the
		// value stored in the file is wrong. Either way the value is very likely corrupted and to
		// make sure a bad value is not read we should return an error.
		if actualChecksum != readChecksum {
			return nil, ErrBadValueChecksum
		}
	}

	return value[:size], nil
}

// Write will take a value and write it to the value file. It will suffix the value with a 32-bit
// checksum that will be used to guarantee the value is not corrupt. The file is not synchronized
// here and must be called manually.
func (f *valueFile) Write(value []byte) (uint64, error) {
	// We add 4 bytes to the total length of the value in order to properly add the checksum suffix.
	size := uint64(len(value) + 4)

	// Increment the offset atomically for this new value, but then subtract this values total size
	// so that we know the actual offset that we need to write it to and the offset we want to
	// return at the end.
	// This should (in theory) allow for concurrent writes to the same file as the only thing that
	// needs to be contested here is the offset value. I believe that the write function for files
	// is thread-safe.
	offset := atomic.AddUint64(&f.Offset, size) - size

	h := fnv.New32()

	// Try to write the value provided to the fnv hash. If it fails then return the error given. But
	// if there is no error and n != the length that should have been written then return an error
	// indicating that a Checksum could not be created.
	if n, err := h.Write(value); err != nil {
		return 0, err
	} else if n != len(value) {
		return 0, ErrCreatingChecksum
	}

	checksum := h.Sum(nil)

	v := append(value, checksum...)
	// Write the value and checksum to the file at the calculated offset.
	if n, err := f.File.WriteAt(v, int64(offset)); err != nil {
		return 0, err
	} else if uint64(n) != size {
		return 0, ErrIncompleteValue
	}

	// If everything has succeeded and the value has been written, then return the offset of the
	// stored value.
	return offset, nil
}
