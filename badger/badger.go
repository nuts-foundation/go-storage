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

package badger

import (
	"bytes"
	"context"
	"errors"
	"github.com/dgraph-io/badger/v3"
	"os"
	"path"
	"sync"
	"time"

	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/sirupsen/logrus"
)

var _ stoabs.ReadTx = (*tx)(nil)
var _ stoabs.WriteTx = (*tx)(nil)
var _ stoabs.Reader = (*badgerShelf)(nil)
var _ stoabs.Writer = (*badgerShelf)(nil)

const defaultFileTimeout = 5 * time.Second

var fileTimeout = defaultFileTimeout

// CreateBadgerStore creates a new Badger-backed KV Store.
func CreateBadgerStore(filePath string, opts ...stoabs.Option) (stoabs.KVStore, error) {
	cfg := stoabs.DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	badgerOpts := badger.DefaultOptions(filePath)
	if cfg.NoSync {
		badgerOpts = badgerOpts.WithInMemory(true).WithDir("").WithValueDir("")
	}
	if cfg.Log != nil {
		// badgerOpts = badgerOpts.WithSyncWrites(true) // hard writes for single node usage
		badgerOpts = badgerOpts.WithLogger(cfg.Log)
	}

	return createBadgerStore(filePath, badgerOpts, cfg)
}

func createBadgerStore(filePath string, options badger.Options, cfg stoabs.Config) (stoabs.KVStore, error) {
	err := os.MkdirAll(path.Dir(filePath), 0644)
	if err != nil {
		return nil, err
	}

	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}

	return Wrap(db, cfg), nil
}

// Wrap creates a KVStore using an existing badger.DB
func Wrap(db *badger.DB, cfg stoabs.Config) stoabs.KVStore {
	return &Store{
		DB:  db,
		log: cfg.Log,
	}
}

type Store struct {
	DB    *badger.DB
	log   *logrus.Logger
	mutex sync.Mutex
}

func (b *Store) Close(ctx context.Context) error {
	return util.CallWithTimeout(ctx, b.DB.Close, func() {
		b.log.Error("Closing of Badger Store timed out, Store may not shut down correctly.")
	})
}

func (b *Store) Write(ctx context.Context, fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	return b.doTX(ctx, func(tx *tx) error {
		return fn(tx)
	}, true, opts)
}

func (b *Store) Read(ctx context.Context, fn func(stoabs.ReadTx) error) error {
	return b.doTX(ctx, func(tx *tx) error {
		return fn(tx)
	}, false, nil)
}

func (b *Store) WriteShelf(ctx context.Context, shelfName string, fn func(writer stoabs.Writer) error) error {
	return b.doTX(ctx, func(tx *tx) error {
		shelf, err := tx.GetShelfWriter(shelfName)
		if err != nil {
			return err
		}
		return fn(shelf)
	}, true, nil)
}

func (b *Store) ReadShelf(ctx context.Context, shelfName string, fn func(reader stoabs.Reader) error) error {
	return b.doTX(ctx, func(tx *tx) error {
		shelf := tx.GetShelfReader(shelfName)
		return fn(shelf)
	}, false, nil)
}

func (b *Store) doTX(ctx context.Context, fn func(tx *tx) error, writable bool, opts []stoabs.TxOption) error {
	if writable {
		b.mutex.Lock()
	}

	// Start transaction, retrieve/create shelf to operate on
	tx := &tx{
		badgerTx: b.DB.NewTransaction(writable),
		ctx:      ctx,
		store:    b,
	}
	defer tx.rollback()

	// Perform TX action(s)
	appError := fn(tx)

	// Writable TXs should be committed, non-writable TXs rolled back
	if !writable {
		tx.rollback()
		return appError
	}
	// Observe result, commit/rollback
	var err error
	if appError == nil {
		b.log.Trace("Committing Badger transaction")
		// Check context cancellation, if not cancelled/expired; commit.
		if ctx.Err() != nil {
			err = ctx.Err()
			tx.rollback()
		} else if writable {
			err = tx.commit()
		}
		b.mutex.Unlock()
		if err != nil {
			stoabs.OnRollbackOption{}.Invoke(opts)
			return util.WrapError(stoabs.ErrCommitFailed, err)
		}

		stoabs.AfterCommitOption{}.Invoke(opts)
	} else {
		b.log.WithError(appError).Warn("Rolling back transaction application due to error")
		tx.rollback()
		stoabs.OnRollbackOption{}.Invoke(opts)
		return appError
	}

	return nil
}

type tx struct {
	ctx       context.Context
	iterators []*badger.Iterator
	mutex     sync.RWMutex
	store     *Store
	badgerTx  *badger.Txn
}

func (b *tx) Unwrap() interface{} {
	return b.badgerTx
}

func (b *tx) GetShelfReader(shelfName string) stoabs.Reader {
	return b.getBucket(shelfName)
}

func (b *tx) GetShelfWriter(shelfName string) (stoabs.Writer, error) {
	return &badgerShelf{name: shelfName, tx: b}, nil
}

func (b *tx) getBucket(shelfName string) stoabs.Reader {
	return &badgerShelf{name: shelfName, tx: b}
}

func (b *tx) Store() stoabs.KVStore {
	return b.store
}

func (b *tx) newIterator() *badger.Iterator {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	iterator := b.badgerTx.NewIterator(badger.DefaultIteratorOptions)
	b.iterators = append(b.iterators, iterator)

	return iterator
}

func (b *tx) rollback() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, it := range b.iterators {
		it.Close()
	}
	b.badgerTx.Discard()
}

func (b *tx) commit() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, it := range b.iterators {
		it.Close()
	}
	return b.badgerTx.Commit()
}

type badgerShelf struct {
	name string
	tx   *tx
}

func (t badgerShelf) key(key stoabs.Key) stoabs.Key {
	myBytes := []byte(t.name)
	newKey := stoabs.BytesKey(append(myBytes, key.Bytes()...))
	return newKey
}

func (t badgerShelf) Get(key stoabs.Key) ([]byte, error) {
	item, err := t.tx.badgerTx.Get(t.key(key).Bytes())
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	value := make([]byte, item.ValueSize())
	item.ValueCopy(value)
	return value, nil
}

func (t badgerShelf) Put(key stoabs.Key, value []byte) error {
	return t.tx.badgerTx.Set(t.key(key).Bytes(), value)
}

func (t badgerShelf) Delete(key stoabs.Key) error {
	return t.tx.badgerTx.Delete(t.key(key).Bytes())
}

// Stats are currently broken
func (t badgerShelf) Stats() stoabs.ShelfStats {
	var onDiskSize, keyCount uint
	tables := t.tx.store.DB.Tables()
	prefix := []byte(t.name)
	for _, ti := range tables {
		if bytes.HasPrefix(ti.Left, prefix) && bytes.HasPrefix(ti.Right, prefix) {
			onDiskSize += uint(ti.OnDiskSize)
			keyCount += uint(ti.KeyCount)
			//uncompressedSize += uint64(ti.UncompressedSize)
		}
	}
	return stoabs.ShelfStats{
		NumEntries: keyCount,
		ShelfSize:  onDiskSize,
	}
}

func (t badgerShelf) Iterate(callback stoabs.CallerFn, keyType stoabs.Key) error {
	// closed by commit or rollback
	it := t.tx.newIterator()
	t.tx.mutex.RLock()
	defer t.tx.mutex.RUnlock()

	prefix := []byte(t.name)
	for it.Seek(prefix); it.ValidForPrefix(prefix) && t.tx.ctx.Err() == nil; it.Next() {
		item := it.Item()
		k := item.Key()
		if err := item.Value(func(v []byte) error {
			kt, err := keyType.FromBytes(k[len(prefix):])
			if err != nil {
				return err
			}
			return callback(kt, v)
		}); err != nil {
			return err
		}
	}
	if t.tx.ctx.Err() != nil {
		println("cancelled err")
		return stoabs.DatabaseError(t.tx.ctx.Err())
	}
	return nil
}

func (t badgerShelf) Range(from stoabs.Key, to stoabs.Key, callback stoabs.CallerFn, stopAtNil bool) error {
	// closed by commit or rollback
	it := t.tx.newIterator()
	t.tx.mutex.RLock()
	defer t.tx.mutex.RUnlock()

	prefix := []byte(t.name)
	var prevKey stoabs.Key
	end := make([]byte, len(t.name)+len(to.Bytes()))
	copy(end, prefix)
	copy(end[4:], to.Bytes())
	for it.Seek(prefix); it.ValidForPrefix(prefix) && bytes.Compare(it.Item().Key(), end) < 0 && t.tx.ctx.Err() == nil; it.Next() {
		item := it.Item()
		k := item.Key()
		key, _ := from.FromBytes(k)
		if stopAtNil && prevKey != nil && !prevKey.Next().Equals(key) {
			// gap found, stop here
			return nil
		}
		err := item.Value(func(v []byte) error {
			kt, err := from.FromBytes(k[len(prefix):])
			if err != nil {
				return err
			}
			return callback(kt, v)
		})
		if err != nil {
			return err
		}
		prevKey = key
	}
	if t.tx.ctx.Err() != nil {
		return stoabs.DatabaseError(t.tx.ctx.Err())
	}
	return nil
}