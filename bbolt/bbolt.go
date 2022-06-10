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
	"fmt"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"os"
	"path"
	"sync/atomic"
	"time"
)

var _ stoabs.ReadTx = (*bboltTx)(nil)
var _ stoabs.WriteTx = (*bboltTx)(nil)
var _ stoabs.Reader = (*bboltShelf)(nil)
var _ stoabs.Writer = (*bboltShelf)(nil)

// CreateBBoltStore creates a new BBolt-backed KV store.
func CreateBBoltStore(filePath string, opts ...stoabs.Option) (stoabs.KVStore, error) {
	cfg := stoabs.Config{}
	for _, opt := range opts {
		opt(&cfg)
	}

	bboltOpts := *bbolt.DefaultOptions
	if cfg.SyncInterval != 0 {
		bboltOpts.NoSync = true
	}
	return createBBoltStore(filePath, &bboltOpts, cfg)
}

func createBBoltStore(filePath string, options *bbolt.Options, cfg stoabs.Config) (*bboltStore, error) {
	err := os.MkdirAll(path.Dir(filePath), os.ModePerm) // TODO: Right permissions?
	if err != nil {
		return nil, err
	}
	db, err := bbolt.Open(filePath, os.FileMode(0640), options) // TODO: Right permissions?
	if err != nil {
		return nil, err
	}
	var log *logrus.Logger
	if cfg.Log != nil {
		log = cfg.Log
	} else {
		log = logrus.StandardLogger()
	}
	result := &bboltStore{
		db:       db,
		log:      log,
		mustSync: &atomic.Value{},
	}
	result.mustSync.Store(false)
	result.ctx, result.cancelCtx = context.WithCancel(context.Background())
	if cfg.SyncInterval > 0 {
		result.startSync(cfg.SyncInterval)
	}
	return result, nil
}

type bboltStore struct {
	db        *bbolt.DB
	log       *logrus.Logger
	mustSync  *atomic.Value
	ctx       context.Context
	cancelCtx context.CancelFunc
}

func (b bboltStore) Close(ctx context.Context) error {
	// Close BBolt and wait for it to finish
	closeError := make(chan error)
	go func() {
		// Explicitly flush to assert all changes have been
		if b.mustSync.Load().(bool) {
			err := b.db.Sync()
			if err != nil {
				closeError <- fmt.Errorf("could not flush to disk: %w", err)
				return
			}
		}

		// Signal internal processes to end
		b.cancelCtx()

		// Then close db
		closeError <- b.db.Close()
	}()
	select {
	case <-ctx.Done():
		// Timeout
		b.log.Error("Closing of BBolt store timed out, store may not shut down correctly.")
		return ctx.Err()
	case err := <-closeError:
		// BBolt store closed, err may be nil if closed successfully
		return err
	}
}

func (b bboltStore) Write(fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx})
	}, true, opts)
}

func (b bboltStore) Read(fn func(stoabs.ReadTx) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx})
	}, false, nil)
}

func (b bboltStore) WriteShelf(shelfName string, fn func(writer stoabs.Writer) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		shelf, err := bboltTx{tx: tx}.GetShelfWriter(shelfName)
		if err != nil {
			return err
		}
		return fn(shelf)
	}, true, nil)
}

func (b bboltStore) ReadShelf(shelfName string, fn func(reader stoabs.Reader) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		shelf, err := bboltTx{tx: tx}.GetShelfReader(shelfName)
		if err != nil {
			return err
		}
		if shelf == nil {
			return nil
		}
		return fn(shelf)
	}, false, nil)
}

func (b bboltStore) doTX(fn func(tx *bbolt.Tx) error, writable bool, optsSlice []stoabs.TxOption) error {
	opts := stoabs.TxOptions(optsSlice)

	// Start transaction, retrieve/create shelf to operate on
	dbTX, err := b.db.Begin(writable)
	if err != nil {
		return err
	}

	// Perform TX action(s)
	appError := fn(dbTX)

	// Writable TXs should be committed, non-writable TXs rolled back
	if !writable {
		err := dbTX.Rollback()
		if err != nil {
			b.log.WithError(err).Error("Could not rollback BBolt transaction")
		}
		return appError
	}
	// Signal the write to be flushed to disk
	b.mustSync.Store(true)
	// Observe result, commit/rollback
	if appError == nil {
		b.log.Trace("Committing BBolt transaction")
		err := dbTX.Commit()
		if err != nil {
			opts.InvokeOnRollback()
			return util.WrapError(stoabs.ErrCommitFailed, err)
		}
		opts.InvokeAfterCommit()
	} else {
		b.log.WithError(appError).Warn("Rolling back transaction application due to error")
		err := dbTX.Rollback()
		if err != nil {
			b.log.WithError(err).Error("Could not rollback BBolt transaction")
		}
		opts.InvokeOnRollback()
		return appError
	}

	return nil
}

func (b bboltStore) startSync(interval time.Duration) {
	ticker := time.NewTicker(interval)
	stop := b.ctx.Done()
	go func(mustSync *atomic.Value, db *bbolt.DB) {
		for {
			select {
			case _ = <-ticker.C:
				if b.mustSync.Swap(false).(bool) {
					err := b.db.Sync()
					if err != nil {
						b.log.WithError(err).Error("Failed to sync to disk")
					}
				}
			case _ = <-stop:
				return
			}
		}
	}(b.mustSync, b.db)
}

type bboltTx struct {
	tx *bbolt.Tx
}

func (b bboltTx) GetShelfReader(shelfName string) (stoabs.Reader, error) {
	return b.getBucket(shelfName)
}

func (b bboltTx) GetShelfWriter(shelfName string) (stoabs.Writer, error) {
	bucket, err := b.tx.CreateBucketIfNotExists([]byte(shelfName))
	if err != nil {
		return nil, err
	}
	return &bboltShelf{bucket: bucket}, nil
}

func (b bboltTx) getBucket(shelfName string) (stoabs.Reader, error) {
	bucket := b.tx.Bucket([]byte(shelfName))
	if bucket == nil {
		return nil, nil
	}
	return &bboltShelf{bucket: bucket}, nil
}

type bboltShelf struct {
	bucket *bbolt.Bucket
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
	}
}

func (t bboltShelf) Iterate(callback stoabs.CallerFn) error {
	cursor := t.bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		// return a copy to avoid data manipulation
		vCopy := append(v[:0:0], v...)
		if err := callback(stoabs.BytesKey(k), vCopy); err != nil {
			return err
		}
	}
	return nil
}

func (t bboltShelf) Range(from stoabs.Key, to stoabs.Key, callback stoabs.CallerFn) error {
	cursor := t.bucket.Cursor()
	for k, v := cursor.Seek(from.Bytes()); k != nil && bytes.Compare(k, to.Bytes()) < 0; k, v = cursor.Next() {
		// return a copy to avoid data manipulation
		vCopy := append(v[:0:0], v...)
		if err := callback(stoabs.BytesKey(k), vCopy); err != nil {
			return err
		}
	}
	return nil
}
