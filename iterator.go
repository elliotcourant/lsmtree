package lsmtree

type Iterator interface {
	Seek(prefix []byte)
	Next()
	Item()
}

type Item interface {
	Key() []byte
	Value() []byte
}
