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
	"path"
	"testing"

	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/stretchr/testify/assert"
)

var key = []byte{1, 2, 3}
var value = []byte{4, 5, 6}

const shelf = "test"

func TestBBolt_Write(t *testing.T) {
	t.Run("write, then read", func(t *testing.T) {
		store, _ := createStore(t)

		err := store.Write(func(tx stoabs.WriteTx) error {
			writer, err := tx.GetShelfWriter(shelf)
			if err != nil {
				return err
			}
			return writer.Put(stoabs.BytesKey(key), value)
		})

		var actual []byte
		err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			actual, err = reader.Get(stoabs.BytesKey(key))
			return err
		})
		assert.NoError(t, err)
		assert.Equal(t, value, actual)
	})

	t.Run("onRollback called, afterCommit not called when commit fails", func(t *testing.T) {
		store, _ := createStore(t)

		var rollbackCalled = false
		var afterCommitCalled = false
		err := store.Write(func(tx stoabs.WriteTx) error {
			_ = tx.(*bboltTx).tx.Rollback()
			return nil
		}, stoabs.OnRollback(func() {
			rollbackCalled = true
		}), stoabs.AfterCommit(func() {
			afterCommitCalled = true
		}))
		assert.Error(t, err)
		assert.True(t, rollbackCalled)
		assert.False(t, afterCommitCalled)
	})

	t.Run("afterCommit and onRollback after commit", func(t *testing.T) {
		store, _ := createStore(t)

		var actual []byte
		var innerError error
		var onRollbackCalled bool

		err := store.Write(func(tx stoabs.WriteTx) error {
			writer, err := tx.GetShelfWriter(shelf)
			if err != nil {
				return err
			}
			return writer.Put(stoabs.BytesKey(key), value)
		}, stoabs.AfterCommit(func() {
			// Happens after commit, so we should be able to read the data now
			innerError = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
				actual, innerError = reader.Get(stoabs.BytesKey(key))
				return innerError
			})
			if innerError != nil {
				t.Fatal(innerError)
			}
		}), stoabs.OnRollback(func() {
			onRollbackCalled = true
		}))

		assert.NoError(t, err)
		assert.Equal(t, value, actual)
		assert.False(t, onRollbackCalled)
	})
	t.Run("afterCommit and onRollback on rollback", func(t *testing.T) {
		store, _ := createStore(t)

		var afterCommitCalled bool
		var onRollbackCalled bool

		_ = store.Write(func(tx stoabs.WriteTx) error {
			return errors.New("failed")
		}, stoabs.AfterCommit(func() {
			afterCommitCalled = true
		}), stoabs.OnRollback(func() {
			onRollbackCalled = true
		}))

		assert.False(t, afterCommitCalled)
		assert.True(t, onRollbackCalled)
	})
	t.Run("additional afterCommit added on writeTx", func(t *testing.T) {
		store, _ := createStore(t)

		var actual []byte
		var innerError error
		var onRollbackCalled bool

		err := store.Write(func(tx stoabs.WriteTx) error {
			tx.AfterCommit(func() {
				// Happens after commit, so we should be able to read the data now
				innerError = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
					actual, innerError = reader.Get(stoabs.BytesKey(key))
					return innerError
				})
				if innerError != nil {
					t.Fatal(innerError)
				}
			})
			writer, err := tx.GetShelfWriter(shelf)
			if err != nil {
				return err
			}
			return writer.Put(stoabs.BytesKey(key), value)
		}, stoabs.OnRollback(func() {
			onRollbackCalled = true
		}))

		assert.NoError(t, err)
		assert.Equal(t, value, actual)
		assert.False(t, onRollbackCalled)
	})
}

func TestBBolt_Read(t *testing.T) {
	t.Run("non-existing shelf", func(t *testing.T) {
		store, _ := createStore(t)

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

func TestBBolt_WriteShelf(t *testing.T) {
	t.Run("write, then read", func(t *testing.T) {
		store, _ := createStore(t)

		// First write
		err := store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			return writer.Put(stoabs.BytesKey(key), value)
		})
		if !assert.NoError(t, err) {
			return
		}

		// Now read
		var actual []byte
		err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			actual, err = reader.Get(stoabs.BytesKey(key))
			return err
		})
		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, value, actual)
	})
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

func TestBBolt_ReadShelf(t *testing.T) {
	t.Run("read from non-existing shelf", func(t *testing.T) {
		store, _ := createStore(t)

		called := false
		err := store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			called = true
			return nil
		})

		assert.NoError(t, err)
		assert.False(t, called)
	})
}

func TestBboltShelf_Iterate(t *testing.T) {
	t.Run("iterates over all keys", func(t *testing.T) {
		store, _ := createStore(t)

		// Write some data
		_ = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			_ = writer.Put(stoabs.BytesKey(value), key)
			return writer.Put(stoabs.BytesKey(key), value)
		})

		var keys, values [][]byte
		err := store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			err := reader.Iterate(func(key stoabs.Key, value []byte) error {
				keys = append(keys, key.Bytes())
				values = append(values, value)
				return nil
			})

			return err
		})
		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 2) {
			return
		}
		if !assert.Len(t, values, 2) {
			return
		}
		// this ordering is how bbolt is sorted: binary tree
		assert.Equal(t, key, keys[0])
		assert.Equal(t, value, keys[1])
		assert.Equal(t, value, values[0])
		assert.Equal(t, key, values[1])
	})

	t.Run("error", func(t *testing.T) {
		store, _ := createStore(t)

		// Write some data otherwise shelf is empty and no error can be returned
		_ = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			return writer.Put(stoabs.BytesKey(key), value)
		})

		err := store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			err := reader.Iterate(func(key stoabs.Key, value []byte) error {
				return errors.New("failure")
			})

			return err
		})
		assert.EqualError(t, err, "failure")
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

func TestBBolt_Close(t *testing.T) {
	t.Run("close closed store", func(t *testing.T) {
		store, _ := createStore(t)
		assert.NoError(t, store.Close(context.Background()))
		assert.NoError(t, store.Close(context.Background()))
	})
	t.Run("timeout", func(t *testing.T) {
		store, _ := createStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := store.Close(ctx)
		assert.Equal(t, err, context.Canceled)
	})
}

func createStore(t *testing.T) (stoabs.KVStore, error) {
	store, err := CreateBBoltStore(path.Join(util.TestDirectory(t), "bbolt.db"), stoabs.WithNoSync())
	t.Cleanup(func() {
		store.Close(context.Background())
	})
	return store, err
}
