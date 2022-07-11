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
		db:  db,
		log: getLogger(cfg),
	}
}

func getLogger(cfg stoabs.Config) *logrus.Logger {
	if cfg.Log != nil {
		return cfg.Log
	}
	return stoabs.DefaultLogger()
}

type store struct {
	db  *bbolt.DB
	log *logrus.Logger
}

func (b *store) Close(ctx context.Context) error {
	if b.db == nil {
		return nil
	}
	defer func() {
		b.db = nil
	}()
	return util.CallWithTimeout(ctx, b.db.Close, func() {
		b.log.Error("Closing of BBolt store timed out, store may not shut down correctly.")
	})
}

func (b *store) Write(fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx, store: b})
	}, true, opts)
}

func (b *store) Read(fn func(stoabs.ReadTx) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx, store: b})
	}, false, nil)
}

func (b *store) WriteShelf(shelfName string, fn func(writer stoabs.Writer) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		shelf, err := bboltTx{tx: tx, store: b}.GetShelfWriter(shelfName)
		if err != nil {
			return err
		}
		return fn(shelf)
	}, true, nil)
}

func (b *store) ReadShelf(shelfName string, fn func(reader stoabs.Reader) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		shelf := bboltTx{tx: tx, store: b}.GetShelfReader(shelfName)
		return fn(shelf)
	}, false, nil)
}

func (b *store) doTX(fn func(tx *bbolt.Tx) error, writable bool, optsSlice []stoabs.TxOption) error {
	if err := b.checkOpen(); err != nil {
		return err
	}

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

func (b *store) checkOpen() error {
	if b.db == nil {
		return stoabs.ErrStoreIsClosed
	}
	return nil
}

type bboltTx struct {
	store *store
	tx    *bbolt.Tx
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
	return &bboltShelf{bucket: bucket}, nil
}

func (b bboltTx) getBucket(shelfName string) stoabs.Reader {
	bucket := b.tx.Bucket([]byte(shelfName))
	if bucket == nil {
		return stoabs.NilReader{}
	}
	return &bboltShelf{bucket: bucket}
}

func (b bboltTx) Store() stoabs.KVStore {
	return b.store
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
		ShelfSize:  uint(t.bucket.Tx().Size()),
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

func (t bboltShelf) Range(from stoabs.Key, to stoabs.Key, callback stoabs.CallerFn, _ bool) error {
	// TODO: stopAtNil is ignored for BBolt
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
