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
	"errors"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

var key = []byte{1, 2, 3}
var value = []byte{4, 5, 6}

const shelf = "test"

func TestBBolt_Write(t *testing.T) {
	t.Run("write, then read", func(t *testing.T) {
		store, _ := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
		defer store.Close()

		err := store.Write(func(tx stoabs.WriteTx) error {
			writer, err := tx.GetShelfWriter(shelf)
			if err != nil {
				return err
			}
			return writer.Put(key, value)
		})

		var actual []byte
		err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			actual, err = reader.Get(key)
			return err
		})
		assert.NoError(t, err)
		assert.Equal(t, value, actual)
	})

	t.Run("afterCommit and afterRollback after commit", func(t *testing.T) {
		store, _ := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
		defer store.Close()

		var actual []byte
		var innerError error
		var afterRollbackCalled bool

		err := store.Write(func(tx stoabs.WriteTx) error {
			writer, err := tx.GetShelfWriter(shelf)
			if err != nil {
				return err
			}
			return writer.Put(key, value)
		}, stoabs.AfterCommit(func() {
			// Happens after commit, so we should be able to read the data now
			innerError = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
				actual, innerError = reader.Get(key)
				return innerError
			})
			if innerError != nil {
				t.Fatal(innerError)
			}
		}), stoabs.AfterRollback(func() {
			afterRollbackCalled = true
		}))

		assert.NoError(t, err)
		assert.Equal(t, value, actual)
		assert.False(t, afterRollbackCalled)
	})
	t.Run("afterCommit and afterRollback on rollback", func(t *testing.T) {
		store, _ := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
		defer store.Close()

		var afterCommitCalled bool
		var afterRollbackCalled bool

		_ = store.Write(func(tx stoabs.WriteTx) error {
			return errors.New("failed")
		}, stoabs.AfterCommit(func() {
			afterCommitCalled = true
		}), stoabs.AfterRollback(func() {
			afterRollbackCalled = true
		}))

		assert.False(t, afterCommitCalled)
		assert.True(t, afterRollbackCalled)
	})
}

func TestBBolt_Read(t *testing.T) {
	t.Run("non-existing shelf", func(t *testing.T) {
		store, _ := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
		defer store.Close()

		err := store.Read(func(tx stoabs.ReadTx) error {
			bucket, err := tx.GetShelfReader(shelf)
			if err != nil {
				return err
			}
			if bucket == nil {
				return nil
			}
			t.Fatal()
			return nil
		})
		assert.NoError(t, err)
	})
}

func TestBBolt_WriteBucket(t *testing.T) {
	t.Run("write, then read", func(t *testing.T) {
		store, _ := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
		defer store.Close()

		// First write
		err := store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			return writer.Put(key, value)
		})
		if !assert.NoError(t, err) {
			return
		}

		// Now read
		var actual []byte
		err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			actual, err = reader.Get(key)
			return err
		})
		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, value, actual)
	})
	t.Run("rollback on application error", func(t *testing.T) {
		store, _ := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
		defer store.Close()

		err := store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			err := writer.Put(key, value)
			if err != nil {
				panic(err)
			}
			return errors.New("failed")
		})
		assert.EqualError(t, err, "failed")

		// Now assert the TX was rolled back
		var actual []byte
		err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			actual, err = reader.Get(key)
			return err
		})
		if !assert.NoError(t, err) {
			return
		}
		assert.Nil(t, actual)
	})
}

func TestBBolt_ReadBucket(t *testing.T) {
	t.Run("read from non-existing shelf", func(t *testing.T) {
		store, _ := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
		defer store.Close()

		called := false
		err := store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			called = true
			return nil
		})

		assert.NoError(t, err)
		assert.False(t, called)
	})
}

func TestBBolt_Close(t *testing.T) {
	t.Run("close closed store", func(t *testing.T) {
		store, _ := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
		assert.NoError(t, store.Close())
		assert.NoError(t, store.Close())
	})
}
