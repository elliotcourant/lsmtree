package lsmtree

// DB is the root object for the database. You can open/create your DB by calling Open().
type DB struct {
	values *valueManager
}

// Open will open or create the database using the provided configuration.
func Open() (*DB, error) {
	panic("not implemented")
}
