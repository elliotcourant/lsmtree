package lsmtree

import (
	"fmt"
)

// Options is used to configure how the database will behave.
type Options struct {
	// MaxWALSegmentSize (in bytes) is the largest a single WAL segment file will grow to before a
	// new segment is started. This does not include the last transaction to be appended to a single
	// WAL segment. If the last transaction puts the segment over this limit then it will still be
	// appended (resulting in a large segment) but then a new segment will be created for subsequent
	// transactions.
	// Default is 8kb.
	MaxWALSegmentSize uint64

	// MaxValueChunkSize (in byteS) is the largest a single Value file will grow to before a new
	// file is created. This does not include the last value appended to the value file.
	// Default is 32kb.
	MaxValueChunkSize uint64

	// WALDirectory is the folder where WAL segment files will be stored.
	// Default is db/wal.
	WALDirectory string

	// DataDirectory is the folder where heap and value files will be stored.
	// Default is db/data.
	DataDirectory string

	// Number of pending writes that can be queued up concurrently before transaction commits will
	// be blocked.
	PendingWritesBuffer int
}

// DB is the root object for the database. You can open/create your DB by calling Open().
type DB struct {
	wal    *walManager
	values *valueManager

	writeChannel     chan interface{}
	stopWriteChannel chan chan error
}

// Open will open or create the database using the provided configuration.
func Open(options Options) (*DB, error) {
	// TODO (elliotcourant) Add options validation.

	// Try to setup the WAL manager.
	wal, err := newWalManager(options.WALDirectory, options.MaxWALSegmentSize)
	if err != nil {
		return nil, err
	}

	db := &DB{
		wal:          wal,
		values:       nil,
		writeChannel: make(chan interface{}, options.PendingWritesBuffer),

		// TODO (elliotcourant) make this channel some sort of cancelFuture object.
		stopWriteChannel: make(chan chan error, 1), // Make this a single byte for now.
	}

	// Start the background writer to accept transaction commits.
	go db.backgroundWriter()

	return db, nil
}

// DefaultOptions just provides a basic configuration which can be passed to open a database.
func DefaultOptions() Options {
	return Options{
		MaxWALSegmentSize:   1024 /* 1kb */ * 8,  /* 8kb */
		MaxValueChunkSize:   1024 /* 1kb */ * 32, /* 32kb */
		DataDirectory:       "db/data",
		WALDirectory:        "db/wal",
		PendingWritesBuffer: 8,
	}
}

// Close will close any open files and stop any background writes. Any writes that have not been
// returned successfully will not have been written to the database.
func (db *DB) Close() error {
	// Create a channel that we can use to wait for the response from the background writer.
	writeChannelFuture := make(chan error, 0)

	// Stop the background writer by sending the channel to it.
	db.stopWriteChannel <- writeChannelFuture

	// Wait to get a response from the background writer.
	if err := <-writeChannelFuture; err != nil {
		return err
	}

	// TODO (elliotcourant) Add timeout logic here if the background writer takes too long to exit.

	return nil
}

func (db *DB) backgroundWriter() {
	for {
		select {
		case txn := <-db.writeChannel:
			fmt.Println(txn)

		case stopResult := <-db.stopWriteChannel:
			// If we receive anything on the stopWriteChannel then just exit this method.
			stopResult <- nil
			return
		}
	}
}
