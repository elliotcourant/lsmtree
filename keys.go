package lsmtree

type (
	// TimestampedKey represents a byte array that will always have an 8 byte suffix to indicate the
	// transactionId for the item. This is used to implement MVCC.
	TimestampedKey []byte

	// Key represents an array that will NOT have an 8 byte suffix that is used to indicate the
	// transactionId for the item.
	Key []byte
)
