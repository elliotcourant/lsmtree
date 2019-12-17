package lsmtree

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
)

var (
	// Make sure that the os.File struct implements the writer and reader at interfaces.
	_ ReaderWriterAt = &os.File{}

	// Make sure that the os.File struct implements the sync interface.
	_ CanSync = &os.File{}
)

type (
	// fileType is a simple 1-Byte value that prefixes all of the file names to indicate the type of
	// file that is being read/written.
	fileType byte

	// ReaderWriterAt is used as the interface for reading and writing data for the database. It can
	// be used in nearly every IO portion of the database.
	ReaderWriterAt interface {
		io.ReaderAt
		io.WriterAt
	}

	// CanSync is used to check if the current IO interface that a file wrapper is using has a
	// method that allows its changes to be flushed to the disk.
	CanSync interface {
		Sync() error
	}
)

const (
	// fileTypeManifest is used as a prefix to designate the manifest file. The manifest file
	// stores the bare minimum information for the database.
	fileTypeManifest fileType = iota

	// fileTypeWal is used as a prefix to designate write-ahead-log files. Write ahead log files
	// are used to keep track of all of the changes made to the database overtime and use to
	// guarantee that a given change is atomic.
	fileTypeWal

	// fileTypeHeap is used as a prefix to designate heap files. Heap files are sorted sets of keys
	// and pointers to a key's value. Heap files are built from memtables and are only flushed to
	// the disk when the memtable reaches a certain size, or if it were to be manually invoked.
	fileTypeHeap

	// fileTypeValue is used as a prefix to designate value files. Value files are larger than heap
	// files and are used as append only storage. They are written much more frequently than heap
	// files and kept in memory for only short periods of time. When a value needs to be retrieved
	// the file will be located in memory and the address of the value within the file will be read
	// or the file will be loaded from the disk and have it's value read.
	fileTypeValue
)

// getPathExists will return true or false indicating whether or not the path specified (file or
// folder) is valid.
func getPathExists(path string) bool {
	// We can do this by getting the stat for the path specified. If we get a NotExist error then we
	// know that the path is not valid.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}

	// If we didn't get an error then the path is valid.
	return true
}

// newDirectory will create a new directory at the path specified, including any missing directories
// in the provided path. The directory will be owned by the current user. If the directory already
// exists then nothing will change.
func newDirectory(path string) error {
	if err := createDirectory(path); err == nil {
		return takeOwnership(path)
	} else {
		return err
	}
}

// createDirectory will create a directory at the path specified. If the path contains multiple
// directories that do not exist, all of them will be created.
func createDirectory(path string) error {
	return os.MkdirAll(path, os.ModeDir)
}

// takeOwnership will change the owner of the path specified to be such that the DB has ownership.
func takeOwnership(path string) error {
	return os.Chown(path, os.Getuid(), os.Getgid())
}

// getValueFileName returns a string representation of the value file name. The name is a
// hexadecimal encoded byte array, with the first byte being the value file type prefix and the
// following 8 bytes being the fileId.
func getValueFileName(fileId uint64) string {
	n := make([]byte, 9)

	// The first byte of the filename is the fileTypeValue const.
	n[0] = byte(fileTypeValue)

	// The following 8 bytes is the fileId itself.
	binary.BigEndian.PutUint64(n[1:], fileId)

	// The plaintext filename is the hexadecimal encoding of the 9 bytes.
	return hex.EncodeToString(n)
}

// getWalSegmentFileName returns a string representation of the WAL segment file name. The name is a
// hexadecimal encoded byte array, with the first byte being the wal file type prefix and the
// following 8 bytes being the segmentId.
func getWalSegmentFileName(segmentId uint64) string {
	n := make([]byte, 9)

	// The first byte of the filename is the fileTypeWal const.
	n[0] = byte(fileTypeWal)

	// The following 8 bytes is the segmentId itself.
	binary.BigEndian.PutUint64(n[1:], segmentId)

	// The plaintext filename is the hexadecimal encoding of the 9 bytes.
	return hex.EncodeToString(n)
}
