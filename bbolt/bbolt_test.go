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
	"context"
	"errors"
	"fmt"
	"github.com/nuts-foundation/go-stoabs/kvtests"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"go.etcd.io/bbolt"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var key = []byte{1, 2, 3}
var value = []byte{4, 5, 6}

const shelf = "test"

func TestBBolt(t *testing.T) {
	provider := func(t *testing.T) (stoabs.KVStore, error) {
		return CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
	}

	kvtests.TestReadingAndWriting(t, provider)
	kvtests.TestRange(t, provider)
	kvtests.TestIterate(t, provider)
	kvtests.TestClose(t, provider)
	kvtests.TestStats(t, provider)
	kvtests.TestWriteTransactions(t, provider)
}

func TestBBolt_Unwrap(t *testing.T) {
	store, _ := createStore(t)

	var tx interface{}
	_ = store.Read(func(innerTx stoabs.ReadTx) error {
		tx = innerTx.Unwrap()
		return nil
	})
	_, ok := tx.(*bbolt.Tx)
	assert.True(t, ok)
}

func TestBBolt_WriteShelf(t *testing.T) {
	t.Run("rollback on application error", func(t *testing.T) {
		store, _ := createStore(t)

		err := store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			err := writer.Put(stoabs.BytesKey(key), value)
			if err != nil {
				panic(err)
			}
			return errors.New("failed")
		})
		assert.EqualError(t, err, "failed")

		// Now assert the TX was rolled back
		var actual []byte
		err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			actual, err = reader.Get(stoabs.BytesKey(key))
			return err
		})
		if !assert.NoError(t, err) {
			return
		}
		assert.Nil(t, actual)
	})
}

func TestBboltShelf_Range(t *testing.T) {
	t.Run("returns correct key/values", func(t *testing.T) {
		store, _ := createStore(t)
		from := stoabs.BytesKey(key) // inclusive
		to := stoabs.BytesKey(value) // exclusive

		// Write some data
		_ = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			_ = writer.Put(stoabs.BytesKey(value), key)
			return writer.Put(stoabs.BytesKey(key), value)
		})

		var keys, values [][]byte
		err := store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			err := reader.Range(from, to, func(key stoabs.Key, value []byte) error {
				keys = append(keys, key.Bytes())
				values = append(values, value)
				return nil
			})

			return err
		})
		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 1) {
			return
		}
		if !assert.Len(t, values, 1) {
			return
		}
		assert.Equal(t, key, keys[0])
		assert.Equal(t, value, values[0])
	})

	t.Run("error", func(t *testing.T) {
		store, _ := createStore(t)
		from := stoabs.BytesKey(key) // inclusive
		to := stoabs.BytesKey(value) // exclusive

		// Write some data
		_ = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			return writer.Put(stoabs.BytesKey(key), value)
		})

		err := store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			err := reader.Range(from, to, func(key stoabs.Key, value []byte) error {
				return errors.New("failure")
			})

			return err
		})

		assert.EqualError(t, err, "failure")
	})
}

func createStore(t *testing.T) (stoabs.KVStore, error) {
	store, err := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
	t.Cleanup(func() {
		_ = store.Close(context.Background())
	})
	return store, err
}

func TestBBolt_CreateBBoltStore(t *testing.T) {
	t.Run("opening locked file logs warning", func(t *testing.T) {
		fileTimeout = 10 * time.Millisecond
		defer func() {
			fileTimeout = defaultFileTimeout
		}()
		filename := filepath.Join(util.TestDirectory(t), "test-store")
		logger, hook := test.NewNullLogger()

		// create first store
		store1, err := CreateBBoltStore(filename)
		if !assert.NoError(t, err) {
			return
		}
		defer store1.Close(context.Background())

		// create second store
		go func() {
			store2, _ := CreateBBoltStore(filename, stoabs.WithLogger(logger)) // hangs while store1 is open
			_ = store2.Close(context.Background())
		}()

		// wait for logger
		var lastEntry *logrus.Entry
		util.WaitFor(t, func() (bool, error) {
			lastEntry = hook.LastEntry()
			return lastEntry != nil, nil
		}, 100*fileTimeout, "time-out while waiting for log message")

		assert.Equal(t, fmt.Sprintf("Trying to open %s, but file appears to be locked", filename), lastEntry.Message)
		assert.Equal(t, logrus.WarnLevel, lastEntry.Level)
	})
}
