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
	"github.com/nuts-foundation/go-storage/api"
	"github.com/nuts-foundation/go-storage/util"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"os"
	"path"
)

var _ api.IterableKVStore = (*bboltStore)(nil)
var _ api.ReadTx = (*bboltTx)(nil)
var _ api.IterableReadTx = (*bboltTx)(nil)
var _ api.WriteTx = (*bboltTx)(nil)
var _ api.Reader = (*bboltShelf)(nil)
var _ api.Writer = (*bboltShelf)(nil)
var _ api.IterableReader = (*bboltShelf)(nil)

//var _ Cursor = (*bboltCursor)(nil)

// CreateBBoltStore creates a new BBolt-backed KV store.
func CreateBBoltStore(filePath string, opts ...api.Option) (api.IterableKVStore, error) {
	cfg := api.Config{}
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

func createBBoltStore(filePath string, options *bbolt.Options, cfg api.Config) (*bboltStore, error) {
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
	return &bboltStore{
		db:  db,
		log: log,
	}, nil
}

type bboltStore struct {
	db  *bbolt.DB
	log *logrus.Logger
}

func (b bboltStore) Close() error {
	return b.db.Close()
}

func (b bboltStore) Write(fn func(api.WriteTx) error, opts ...api.TxOption) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx})
	}, true, opts)
}

func (b bboltStore) Read(fn func(api.ReadTx) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx})
	}, false, nil)
}

func (b bboltStore) ReadIterable(fn func(api.IterableReadTx) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		return fn(&bboltTx{tx: tx})
	}, false, nil)
}

func (b bboltStore) WriteShelf(shelfName string, fn func(writer api.Writer) error) error {
	return b.doTX(func(tx *bbolt.Tx) error {
		shelf, err := bboltTx{tx: tx}.GetShelfWriter(shelfName)
		if err != nil {
			return err
		}
		return fn(shelf)
	}, true, nil)
}

func (b bboltStore) ReadShelf(shelfName string, fn func(reader api.Reader) error) error {
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

func (b bboltStore) doTX(fn func(tx *bbolt.Tx) error, writable bool, optsSlice []api.TxOption) error {
	opts := api.TxOptions(optsSlice)

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
			return util.WrapError(api.ErrCommitFailed, err)
		}
		opts.InvokeAfterCommit()
	} else {
		b.log.WithError(appError).Warn("Rolling back transaction application due to error")
		err := dbTX.Rollback()
		if err != nil {
			b.log.WithError(err).Error("Could not rollback BBolt transaction")
		} else {
			opts.InvokeAfterRollback()
		}
		return appError
	}

	return nil
}

type bboltTx struct {
	tx *bbolt.Tx
}

func (b bboltTx) GetShelfReader(shelfName string) (api.Reader, error) {
	return b.getBucket(shelfName)
}

func (b bboltTx) FromIterableShelf(shelfName string) (api.IterableReader, error) {
	return b.getBucket(shelfName)
}

func (b bboltTx) GetShelfWriter(shelfName string) (api.Writer, error) {
	bucket, err := b.tx.CreateBucketIfNotExists([]byte(shelfName))
	if err != nil {
		return nil, err
	}
	return &bboltShelf{bucket: bucket}, nil
}

func (b bboltTx) getBucket(shelfName string) (api.IterableReader, error) {
	bucket := b.tx.Bucket([]byte(shelfName))
	if bucket == nil {
		return nil, nil
	}
	return &bboltShelf{bucket: bucket}, nil
}

type bboltShelf struct {
	bucket *bbolt.Bucket
}

func (t bboltShelf) Cursor() (api.Cursor, error) {
	return t.bucket.Cursor(), nil
}

func (t bboltShelf) Get(key []byte) ([]byte, error) {
	value := t.bucket.Get(key)
	// Because things will go terribly wrong when you use a []byte returned by BBolt outside its transaction,
	// we want to make sure to work with a copy.
	//
	// This seems to be the best (and shortest) way to copy a byte slice:
	// https://github.com/go101/go101/wiki/How-to-perfectly-clone-a-slice%3F
	return append(value[:0:0], value...), nil
}

func (t bboltShelf) Put(key []byte, value []byte) error {
	return t.bucket.Put(key, value)
}

func (t bboltShelf) Delete(key []byte) error {
	return t.bucket.Delete(key)
}

func (t bboltShelf) Stats() api.ShelfStats {
	return api.ShelfStats{
		NumEntries: uint(t.bucket.Stats().KeyN),
	}
}
