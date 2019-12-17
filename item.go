package lsmtree

type Item struct {
	Key     Key
	Value   []byte
	Version uint64
}
