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

package bbolt

import (
	"bytes"
	"context"
	"errors"
	"github.com/viney-shih/go-lock"
	"os"
	"path"
	"time"

	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

var _ stoabs.ReadTx = (*bboltTx)(nil)
var _ stoabs.WriteTx = (*bboltTx)(nil)
var _ stoabs.Reader = (*bboltShelf)(nil)
var _ stoabs.Writer = (*bboltShelf)(nil)

const defaultFileTimeout = 5 * time.Second

var fileTimeout = defaultFileTimeout

// CreateBBoltStore creates a new BBolt-backed KV store.
func CreateBBoltStore(filePath string, opts ...stoabs.Option) (stoabs.KVStore, error) {
	cfg := stoabs.Config{}
	for _, opt := range opts {
		opt(&cfg)
	}

	bboltOpts := *bbolt.DefaultOptions
	if cfg.NoSync {
		bboltOpts.NoSync = true
		bboltOpts.NoFreelistSync = true
		bboltOpts.NoGrowSync = true
	}
	return createBBoltStore(filePath, &bboltOpts, cfg)
}

func createBBoltStore(filePath string, options *bbolt.Options, cfg stoabs.Config) (stoabs.KVStore, error) {
	err := os.MkdirAll(path.Dir(filePath), os.ModePerm) // TODO: Right permissions?
	if err != nil {
		return nil, err
	}

	// log warning if file opening hangs
	cfg.Log = getLogger(cfg)
	done := make(chan bool, 1)
	ticker := time.NewTicker(fileTimeout)
	go func() {
		for {
			select {
			case <-done:
				ticker.Stop()
				return
			case <-ticker.C:
				cfg.Log.Warnf("Trying to open %s, but file appears to be locked", filePath)
			}
		}
	}()

	db, err := bbolt.Open(filePath, os.FileMode(0640), options) // TODO: Right permissions?
	done <- true
	if err != nil {
		return nil, err
	}

	return Wrap(db, cfg), nil
}

// Wrap creates a KVStore using an existing bbolt.DB
func Wrap(db *bbolt.DB, cfg stoabs.Config) stoabs.KVStore {
	return &store{
		db:    db,
		mutex: lock.NewCASMutex(),
		log:   getLogger(cfg),
	}
}

func getLogger(cfg stoabs.Config) *logrus.Logger {
	if cfg.Log != nil {
		return cfg.Log
	}
	return stoabs.DefaultLogger()
}

type store struct {
	db    *bbolt.DB
	mutex lock.RWMutex
	log   *logrus.Logger
}

func (b *store) Close(ctx context.Context) error {
	return util.CallWithTimeout(ctx, b.db.Close, func() {
		b.log.Error("Closing of BBolt store timed out, store may not shut down correctly.")
	})
}

func (b *store) Write(ctx context.Context, fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	return b.doTX(ctx, func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx, store: b, ctx: ctx})
	}, true, opts)
}

func (b *store) Read(ctx context.Context, fn func(stoabs.ReadTx) error) error {
	return b.doTX(ctx, func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx, store: b, ctx: ctx})
	}, false, nil)
}

func (b *store) WriteShelf(ctx context.Context, shelfName string, fn func(writer stoabs.Writer) error) error {
	return b.doTX(ctx, func(tx *bbolt.Tx) error {
		shelf, err := bboltTx{tx: tx, store: b, ctx: ctx}.GetShelfWriter(shelfName)
		if err != nil {
			return err
		}
		return fn(shelf)
	}, true, nil)
}

func (b *store) ReadShelf(ctx context.Context, shelfName string, fn func(reader stoabs.Reader) error) error {
	return b.doTX(ctx, func(tx *bbolt.Tx) error {
		shelf := bboltTx{tx: tx, store: b, ctx: ctx}.GetShelfReader(shelfName)
		return fn(shelf)
	}, false, nil)
}

func (b *store) doTX(ctx context.Context, fn func(tx *bbolt.Tx) error, writable bool, optsSlice []stoabs.TxOption) error {
	opts := stoabs.TxOptions(optsSlice)

	// custom mutex
	subCtx, _ := context.WithTimeout(ctx, time.Second)

	if writable {
		if !b.mutex.TryLockWithContext(subCtx) {
			return errors.New("timeout while waiting for lock")
		}
	} else {
		if !b.mutex.RTryLockWithContext(subCtx) {
			return errors.New("timeout while waiting for lock")
		}
	}

	// Start transaction, retrieve/create shelf to operate on
	dbTX, err := b.db.Begin(writable)
	if err != nil {
		return err
	}

	// Perform TX action(s)
	appError := fn(dbTX)

	// Writable TXs should be committed, non-writable TXs rolled back
	if !writable {
		rollbackTX(dbTX, b.log)
		b.mutex.RUnlock()
		return appError
	}
	// Observe result, commit/rollback
	if appError == nil {
		b.log.Trace("Committing BBolt transaction")
		// Check context cancellation, if not cancelled/expired; commit.
		if ctx.Err() != nil {
			err = ctx.Err()
			rollbackTX(dbTX, b.log)
		} else {
			err = dbTX.Commit()
		}
		b.mutex.Unlock()
		if err != nil {
			opts.InvokeOnRollback()
			return util.WrapError(stoabs.ErrCommitFailed, err)
		}

		opts.InvokeAfterCommit()
	} else {
		b.log.WithError(appError).Warn("Rolling back transaction application due to error")
		rollbackTX(dbTX, b.log)
		opts.InvokeOnRollback()
		b.mutex.Unlock()
		return appError
	}

	return nil
}

func rollbackTX(dbTX *bbolt.Tx, log *logrus.Logger) {
	err := dbTX.Rollback()
	if err != nil {
		log.WithError(err).Error("Could not rollback BBolt transaction")
	}
}

type bboltTx struct {
	store *store
	tx    *bbolt.Tx
	ctx   context.Context
}

func (b bboltTx) Unwrap() interface{} {
	return b.tx
}

func (b bboltTx) GetShelfReader(shelfName string) stoabs.Reader {
	return b.getBucket(shelfName)
}

func (b bboltTx) GetShelfWriter(shelfName string) (stoabs.Writer, error) {
	bucket, err := b.tx.CreateBucketIfNotExists([]byte(shelfName))
	if err != nil {
		return nil, err
	}
	return &bboltShelf{bucket: bucket, ctx: b.ctx}, nil
}

func (b bboltTx) getBucket(shelfName string) stoabs.Reader {
	bucket := b.tx.Bucket([]byte(shelfName))
	if bucket == nil {
		return stoabs.NilReader{}
	}
	return &bboltShelf{bucket: bucket, ctx: b.ctx}
}

func (b bboltTx) Store() stoabs.KVStore {
	return b.store
}

type bboltShelf struct {
	bucket *bbolt.Bucket
	ctx    context.Context
}

func (t bboltShelf) Get(key stoabs.Key) ([]byte, error) {
	value := t.bucket.Get(key.Bytes())
	// Because things will go terribly wrong when you use a []byte returned by BBolt outside its transaction,
	// we want to make sure to work with a copy.
	//
	// This seems to be the best (and shortest) way to copy a byte slice:
	// https://github.com/go101/go101/wiki/How-to-perfectly-clone-a-slice%3F
	return append(value[:0:0], value...), nil
}

func (t bboltShelf) Put(key stoabs.Key, value []byte) error {
	return t.bucket.Put(key.Bytes(), value)
}

func (t bboltShelf) Delete(key stoabs.Key) error {
	return t.bucket.Delete(key.Bytes())
}

func (t bboltShelf) Stats() stoabs.ShelfStats {
	return stoabs.ShelfStats{
		NumEntries: uint(t.bucket.Stats().KeyN),
		ShelfSize:  uint(t.bucket.Tx().Size()),
	}
}

func (t bboltShelf) Iterate(callback stoabs.CallerFn, keyType stoabs.Key) error {
	cursor := t.bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		// Potentially long-running operation, check context for cancellation
		if t.ctx.Err() != nil {
			return t.ctx.Err()
		}
		// return a copy to avoid data manipulation
		vCopy := append(v[:0:0], v...)
		key, err := keyType.FromBytes(k)
		if err != nil {
			return nil
		}
		if err := callback(key, vCopy); err != nil {
			return err
		}
	}
	return nil
}

func (t bboltShelf) Range(from stoabs.Key, to stoabs.Key, callback stoabs.CallerFn, stopAtNil bool) error {
	cursor := t.bucket.Cursor()
	var prevKey stoabs.Key
	for k, v := cursor.Seek(from.Bytes()); k != nil && bytes.Compare(k, to.Bytes()) < 0; k, v = cursor.Next() {
		// Potentially long-running operation, check context for cancellation
		if t.ctx.Err() != nil {
			return t.ctx.Err()
		}
		key, err := from.FromBytes(k)
		if err != nil {
			return err
		}
		if stopAtNil && prevKey != nil && !prevKey.Next().Equals(key) {
			// gap found, stop here
			return nil
		}
		// return a copy to avoid data manipulation
		vCopy := append(v[:0:0], v...)
		if err := callback(key, vCopy); err != nil {
			return err
		}
		prevKey = key
	}
	return nil
}
