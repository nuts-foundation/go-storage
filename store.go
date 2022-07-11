/*
 * Copyright (C) 2022 Nuts community
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package stoabs

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
)

// ErrStoreIsClosed is returned when an operation is executed on a closed store.
var ErrStoreIsClosed = errors.New("store is closed")

// KVStore defines the interface for a key-value store.
// Writing to it is done in callbacks passed to the Write-functions. If the callback returns an error, the transaction is rolled back.
type KVStore interface {
	Store
	// Write starts a writable transaction and passes it to the given function.
	// Callers should not try to read values which are written in the same transactions, and thus haven't been committed yet.
	// The result when doing so depends on transaction isolation of the underlying database.
	Write(fn func(WriteTx) error, opts ...TxOption) error
	// Read starts a read-only transaction and passes it to the given function.
	Read(fn func(ReadTx) error) error
	// WriteShelf starts a writable transaction, open a writer for the specified shelf and passes it to the given function.
	// If the shelf does not exist, it will be created.
	// The same semantics of Write apply.
	WriteShelf(shelfName string, fn func(Writer) error) error
	// ReadShelf starts a read-only transaction, open a reader for the specified shelf and passes it to the given function.
	// If the shelf does not exist, the function is not called.
	ReadShelf(shelfName string, fn func(Reader) error) error
}

type Option func(config *Config)

type Config struct {
	Log    *logrus.Logger
	NoSync bool
	// PageSize contains the maximum number of entries a page sent over the network will contain.
	PageSize int
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

func WithPageSize(pageSize int) Option {
	return func(config *Config) {
		config.PageSize = pageSize
	}
}

// DefaultLogger is the logger that will be used when none is provided to a store
func DefaultLogger() *logrus.Logger {
	return logrus.StandardLogger()
}

// ShelfStats contains statistics about a shelf.
type ShelfStats struct {
	// NumEntries holds the number of entries in the shelf.
	NumEntries uint
	// ShelfSize holds the current shelf size in bytes.
	ShelfSize uint
}

// CallerFn is the function type which is called for each key value pair when using Iterate() or Range()
type CallerFn func(key Key, value []byte) error

// Reader is used to read from a shelf.
type Reader interface {
	// Get returns the value for the given key. If it does not exist it returns nil.
	Get(key Key) ([]byte, error)
	// Iterate walks over all key/value pairs for this shelf. Ordering is not guaranteed.
	Iterate(callback CallerFn) error
	// Range calls the callback for each key/value pair on this shelf from (inclusive) and to (exclusive) given keys.
	// Ordering is guaranteed and determined by the type of Key given.
	Range(from Key, to Key, callback CallerFn) error
	// Stats returns statistics about the shelf.
	Stats() ShelfStats
}

// Writer is used to write to a shelf.
type Writer interface {
	Reader

	// Put stores the given key and value in the shelf.
	Put(key Key, value []byte) error
	// Delete removes the given key from the shelf.
	Delete(key Key) error
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

type writeLockOption struct {
}

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

// RequestsWriteLock returns whether the WithWriteLock option was specified.
func (opts TxOptions) RequestsWriteLock() bool {
	for _, opt := range opts {
		if _, ok := opt.(writeLockOption); ok {
			return true
		}
	}
	return false
}

// WithWriteLock is a transaction option that acquires a write lock for the entire store, making sure there are no concurrent writeable transactions.
// The lock is released when the transaction finishes in any way (commit/rollback).
func WithWriteLock() TxOption {
	return writeLockOption{}
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
	// GetShelfReader returns the specified shelf for reading. If it doesn't exist, a NilReader is returned that will return nil for all read operations.
	GetShelfReader(shelfName string) Reader
	// Store returns the KVStore on which the transaction is started
	Store() KVStore
	// Unwrap returns the underlying, database specific transaction object. If not supported, it returns nil.
	Unwrap() interface{}
}

// NilReader is a shelfReader that always returns nil. It can be used when shelves do not exist.
type NilReader struct{}

func (n NilReader) Get(_ Key) ([]byte, error) {
	return nil, nil
}

func (n NilReader) Iterate(_ CallerFn) error {
	return nil
}

func (n NilReader) Range(_ Key, to Key, _ CallerFn) error {
	return nil
}

func (n NilReader) Stats() ShelfStats {
	return ShelfStats{
		NumEntries: 0,
		ShelfSize:  0,
	}
}
