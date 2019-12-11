package lsmtree

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"os"
	"path"
)

var (
	// ErrBadValueChecksum is returned when a value is read from the value file, but the checksum
	// stored with the value does not match the calculated checksum of the value read. This is used
	// as an indicator of file corruption.
	ErrBadValueChecksum = errors.New("bad value checksum")

	// ErrBrokenValue is returned when the entire value could not be read from from the value file.
	ErrIncompleteValue = errors.New("broken value")
)

type (
	// valueFile represents an append only file that is used to store actual values for the
	// database. Each file is a chunk of the total database file and contains an array of twe types
	// of data. The value itself (which is stored as a raw byte array) and a checksum for the value.
	valueFile struct {
		FileId uint64
		Offset uint64
		File   *os.File
	}
)

// openValueFile will open a value file with the Id specified. If the file does not exist it will
// create the file. The file is opened with the append, create and read/write flags, and the append
// and exclusive mode.
func openValueFile(directory string, fileId uint64) (*valueFile, error) {
	filePath := path.Join(directory, getValueFileName(fileId))
	flags := os.O_APPEND | os.O_CREATE | os.O_RDWR
	mode := os.ModeAppend | os.ModeExclusive
	file, err := os.OpenFile(filePath, flags, mode)
	if err != nil {
		return nil, err
	}

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
	value := make([]byte, size + 4)

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
