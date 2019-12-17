package lsmtree

type Itr interface {
	Seek(prefix []byte)
	Next()
	Item() Item
}
