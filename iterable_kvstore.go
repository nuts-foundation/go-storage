package stoabs

// IterableKVStore is like KVStore but supports iterating over shelves.
type IterableKVStore interface {
	KVStore

	// ReadIterable starts a read-only transaction and passes it to the given function.
	ReadIterable(fn func(IterableReadTx) error) error
}

// IterableReader is a Reader that can also iterate over a shelf using a cursor.
type IterableReader interface {
	Reader

	// Cursor returns a cursor for iterating over the shelf.
	Cursor() (Cursor, error)
}

// IterableReadTx is like ReadTX, but iterable.
type IterableReadTx interface {
	// FromIterableShelf returns the specified shelf for reading. If it doesn't exist, nil is returned.
	FromIterableShelf(shelfName string) (IterableReader, error)
}

