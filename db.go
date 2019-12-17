package lsmtree

// Options is used to configure how the database will behave.
type Options struct {
	// MaxWALSegmentSize (in bytes) is the largest a single WAL segment file will grow to before a
	// new segment is started. This does not include the last transaction to be appended to a single
	// WAL segment. If the last transaction puts the segment over this limit then it will still be
	// appended (resulting in a large segment) but then a new segment will be created for subsequent
	// transactions.
	MaxWALSegmentSize uint64

	// MaxValueChunkSize (in byteS) is the largest a single Value file will grow to before a new
	// file is created. This does not include the last value appended to the value file.
	MaxValueChunkSize uint64

	// WALDirectory is the folder where WAL segment files will be stored.
	WALDirectory string

	// DataDirectory is the folder where heap and value files will be stored.
	DataDirectory string
}

// DB is the root object for the database. You can open/create your DB by calling Open().
type DB struct {
	wal    *walManager
	values *valueManager
}

// Open will open or create the database using the provided configuration.
func Open() (*DB, error) {
	panic("not implemented")
}

// DefaultOptions just provides a basic configuration which can be passed to open a database.
func DefaultOptions() Options {
	return Options{
		MaxWALSegmentSize: 1024 /* 1kb */ * 8,  /* 8kb */
		MaxValueChunkSize: 1024 /* 1kb */ * 32, /* 32kb */
		DataDirectory:     "db/data",
		WALDirectory:      "db/wal",
	}
}
