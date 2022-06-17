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

// CallerFn is the function type which is called for each key value pair when using Iterate() or Range()
type CallerFn func(key Key, value []byte) error

// Reader is used to read from a shelf.
type Reader interface {
	// Get returns the value for the given key. If it does not exist it returns nil.
	Get(key Key) ([]byte, error)
	// Iterate walks over all key/value pairs for this shelf. Ordering is not guaranteed.
	Iterate(callback CallerFn) error
	// Stats returns statistics about the shelf.
	Stats() ShelfStats
	// Range calls the callback for each key/value pair on this shelf from (inclusive) and to (exclusive) given keys.
	// Ordering is guaranteed and determined by the type of Key given.
	Range(from Key, to Key, callback CallerFn) error
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
	// AfterCommit adds an afterCommit function to the active transaction
	AfterCommit(fn func())
}

// ReadTx is used to read from a KVStore.
type ReadTx interface {
	// GetShelfReader returns the specified shelf for reading. If it doesn't exist, nil is returned.
	GetShelfReader(shelfName string) (Reader, error)
}
