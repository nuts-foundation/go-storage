package stoabs

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
)

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
	Log    *logrus.Logger
	NoSync bool
}

func WithNoSync() Option {
	return func(config *Config) {
		config.NoSync = true
	}
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

// ErrCommitFailed is returned when the commit of transaction fails.
var ErrCommitFailed = errors.New("unable to commit transaction")

type Store interface {
	// Close releases all resources associated with the store. It is safe to call multiple (subsequent) times.
	// The context being passed can be used to specify a timeout for the close operation.
	Close(ctx context.Context) error
}

// TxOption holds options for store transactions.
type TxOption interface{}

type TxOptions []TxOption

func (opts TxOptions) InvokeOnRollback() {
	for _, opt := range opts {
		if ar, ok := opt.(*OnRollbackOpt); ok {
			ar.Func()
		}
	}
}

func (opts TxOptions) InvokeAfterCommit() {
	for _, opt := range opts {
		if ar, ok := opt.(*AfterCommitOpt); ok {
			ar.Func()
		}
	}
}

type AfterCommitOpt struct {
	Func func()
}

// AfterCommit specifies a function that will be called after a transaction is successfully committed.
// There can be multiple AfterCommit functions, which will be called in order.
func AfterCommit(fn func()) TxOption {
	return &AfterCommitOpt{Func: fn}
}

type OnRollbackOpt struct {
	Func func()
}

// OnRollback specifies a function that will be called after a transaction is successfully rolled back.
// There can be multiple OnRollback functions, which will be called in order.
func OnRollback(fn func()) TxOption {
	return &OnRollbackOpt{Func: fn}
}

// WriteTx is used to write to a KVStore.
type WriteTx interface {
	ReadTx
	// GetShelfWriter returns the specified shelf for writing. If it doesn't exist, it will be created.
	GetShelfWriter(shelfName string) (Writer, error)
}

// ReadTx is used to read from a KVStore.
type ReadTx interface {
	// GetShelfReader returns the specified shelf for reading. If it doesn't exist, nil is returned.
	GetShelfReader(shelfName string) (Reader, error)
}
