package api

import "github.com/sirupsen/logrus"

// KVStore defines the interface for a key-value store.
// Writing to it is done in callbacks passed to the Write-functions. If the callback returns an error, the transaction is rolled back.
type KVStore interface {
	Store
	// Write starts a writable transaction and passes it to the given function.
	Write(fn func(WriteTx) error, opts ...TxOption) error
	// Read starts a read-only transaction and passes it to the given function.
	Read(fn func(ReadTx) error) error
	// WriteShelf starts a writable transaction, open a writer for the specified shelf and passes it to the given function.
	// If the shelf does not exist, it will be created.
	WriteShelf(shelfName string, fn func(Writer) error) error
	// ReadShelf starts a read-only transaction, open a reader for the specified shelf and passes it to the given function.
	// If the shelf does not exist, the function is not called.
	ReadShelf(shelfName string, fn func(Reader) error) error
}

type Option func(config *Config)

type Config struct {
	Log *logrus.Logger
}

func WithLogger(log *logrus.Logger) Option {
	return func(config *Config) {
		config.Log = log
	}
}

// ShelfStats contains statistics about a shelf.
type ShelfStats struct {
	// NumEntries holds the number of entries in the shelf.
	NumEntries uint
}

// Reader is used to read from a shelf.
type Reader interface {
	// Get returns the value for the given key. If it does not exist it returns nil.
	Get(key []byte) ([]byte, error)
	// Stats returns statistics about the shelf.
	Stats() ShelfStats
}

// Writer is used to write to a shelf.
type Writer interface {
	Reader

	// Put stores the given key and value in the shelf.
	Put(key []byte, value []byte) error
	// Delete removes the given key from the shelf.
	Delete(key []byte) error
}

// Cursor defines the API for iterating over data in a shelf.
type Cursor interface {
	// Seek moves the cursor to the specified key.
	Seek(seek []byte) (key []byte, value []byte)
	// Next moves the cursor to the next key.
	Next() (key []byte, value []byte)
}
