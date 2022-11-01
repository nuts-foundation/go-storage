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
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/kvtests"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"path"
	"path/filepath"
	"testing"
)

var key = []byte{1, 2, 3}
var value = []byte{4, 5, 6}

const shelf = "test"

func TestBadger(t *testing.T) {
	provider := func(t *testing.T) (stoabs.KVStore, error) {
		return CreateBadgerStore(path.Join(util.TestDirectory(t), "badger.DB"), stoabs.WithNoSync())
	}

	kvtests.TestReadingAndWriting(t, provider)
	kvtests.TestRange(t, provider)
	kvtests.TestIterate(t, provider)
	kvtests.TestClose(t, provider)
	kvtests.TestDelete(t, provider)
	//kvtests.TestStats(t, provider) //not yet completed
	kvtests.TestWriteTransactions(t, provider)
	kvtests.TestTransactionWriteLock(t, provider)
}

func TestBadger_Unwrap(t *testing.T) {
	store, _ := createStore(t)

	var tx interface{}
	_ = store.Read(context.Background(), func(innerTx stoabs.ReadTx) error {
		tx = innerTx.Unwrap()
		return nil
	})
	_, ok := tx.(*badger.Txn)
	assert.True(t, ok)
}

func TestBadger_WriteShelf(t *testing.T) {
	ctx := context.Background()

	t.Run("rollback on application error", func(t *testing.T) {
		store, _ := createStore(t)

		err := store.WriteShelf(ctx, shelf, func(writer stoabs.Writer) error {
			err := writer.Put(stoabs.BytesKey(key), value)
			if err != nil {
				panic(err)
			}
			return errors.New("failed")
		})
		assert.EqualError(t, err, "failed")

		// Now assert the TX was rolled back
		var actual []byte
		err = store.ReadShelf(ctx, shelf, func(reader stoabs.Reader) error {
			actual, err = reader.Get(stoabs.BytesKey(key))
			return err
		})
		if !assert.NoError(t, err) {
			return
		}
		assert.Nil(t, actual)
	})
}

func createStore(t *testing.T) (stoabs.KVStore, error) {
	store, err := CreateBadgerStore(path.Join(util.TestDirectory(t), "badger.DB"), stoabs.WithNoSync())
	t.Cleanup(func() {
		_ = store.Close(context.Background())
	})
	return store, err
}

func TestBadger_CreateBadgerStore(t *testing.T) {
	t.Run("opening locked file logs warning", func(t *testing.T) {
		filename := filepath.Join(util.TestDirectory(t), "test-BadgerStore")
		logger, _ := test.NewNullLogger()

		// create first BadgerStore
		store1, err := CreateBadgerStore(filename)
		if !assert.NoError(t, err) {
			return
		}
		defer store1.Close(context.Background())

		_, err = CreateBadgerStore(filename, stoabs.WithLogger(logger)) // hangs while store1 is open

		assert.EqualError(t, err, fmt.Sprintf("Cannot acquire directory lock on \"%s\".  Another process is using this Badger database. error: resource temporarily unavailable", filename))
	})
}
