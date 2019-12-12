package lsmtree

import (
	"encoding/binary"
	"encoding/hex"
	"io"
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

// getValueFileName returns a string representation of the value file name. The name is a
// hexadecimal encoded byte array, with the first byte being the value file type prefix and the
// following 8 bytes being the fileId.
func getValueFileName(fileId uint64) string {
	n := make([]byte, 9)
	n[0] = byte(fileTypeValue)
	binary.BigEndian.PutUint64(n[1:], fileId)
	return hex.EncodeToString(n)
}
