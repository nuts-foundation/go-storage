package kvtests

import (
	"context"
	"errors"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/stretchr/testify/assert"
	"testing"
)

var bytesKey = []byte{1, 2, 3}
var bytesValue = stoabs.BytesKey(bytesKey).Next().Bytes()
var largerBytesKey = []byte{4, 5, 6}
var largerBytesValue = []byte{100, 101, 102}

const stringKey = "message"
const stringValue = "Hello, World!"

const shelf = "test"

type StoreProvider func(t *testing.T) (stoabs.KVStore, error)

func TestReadingAndWriting(t *testing.T, storeProvider StoreProvider) {
	t.Run("read/write", func(t *testing.T) {
		t.Run("write, then read ([]byte])", func(t *testing.T) {
			store := createStore(t, storeProvider)

			err := store.Write(func(tx stoabs.WriteTx) error {
				writer, err := tx.GetShelfWriter(shelf)
				if err != nil {
					return err
				}
				return writer.Put(stoabs.BytesKey(bytesKey), bytesValue)
			})
			if !assert.NoError(t, err) {
				return
			}

			var actual []byte
			err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
				actual, err = reader.Get(stoabs.BytesKey(bytesKey))
				return err
			})
			assert.NoError(t, err)
			assert.Equal(t, bytesValue, actual)
		})
		t.Run("write, then read (string)", func(t *testing.T) {
			store := createStore(t, storeProvider)

			err := store.Write(func(tx stoabs.WriteTx) error {
				writer, err := tx.GetShelfWriter(shelf)
				if err != nil {
					return err
				}
				return writer.Put(stoabs.BytesKey(stringKey), []byte(stringValue))
			})
			if !assert.NoError(t, err) {
				return
			}

			var actual []byte
			err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
				actual, err = reader.Get(stoabs.BytesKey(stringKey))
				return err
			})
			assert.NoError(t, err)
			assert.Equal(t, stringValue, string(actual))
		})

		t.Run("read from non-existing shelf", func(t *testing.T) {
			store := createStore(t, storeProvider)

			called := false
			err := store.ReadShelf(shelf, func(reader stoabs.Reader) error {
				called = true
				return nil
			})

			assert.NoError(t, err)
			assert.False(t, called)
		})
		t.Run("read non-existing key", func(t *testing.T) {
			store := createStore(t, storeProvider)

			err := store.WriteShelf(shelf, func(writer stoabs.Writer) error {
				return writer.Put(stoabs.BytesKey(stringKey), []byte(stringValue))
			})
			if !assert.NoError(t, err) {
				return
			}

			var actual []byte
			err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
				actual, err = reader.Get(stoabs.BytesKey(bytesKey))
				return err
			})
			if !assert.NoError(t, err) {
				return
			}
			assert.Nil(t, actual)
		})
	})
}

func TestRange(t *testing.T, storeProvider StoreProvider) {
	t.Run("range", func(t *testing.T) {
		t.Run("returns correct key/values", func(t *testing.T) {
			store := createStore(t, storeProvider)
			from := stoabs.BytesKey(bytesKey)     // inclusive
			to := stoabs.BytesKey(largerBytesKey) // exclusive

			// Write some data
			_ = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
				_ = writer.Put(stoabs.BytesKey(largerBytesKey), bytesValue)
				return writer.Put(stoabs.BytesKey(bytesKey), bytesValue)
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
			assert.Equal(t, bytesKey, keys[0])
			assert.Equal(t, bytesValue, values[0])
		})

		t.Run("error", func(t *testing.T) {
			store := createStore(t, storeProvider)
			from := stoabs.BytesKey(bytesKey)     // inclusive
			to := stoabs.BytesKey(largerBytesKey) // exclusive

			// Write some data
			_ = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
				return writer.Put(stoabs.BytesKey(bytesKey), bytesValue)
			})

			err := store.ReadShelf(shelf, func(reader stoabs.Reader) error {
				err := reader.Range(from, to, func(key stoabs.Key, value []byte) error {
					return errors.New("failure")
				})

				return err
			})

			assert.EqualError(t, err, "failure")
		})
	})
}

func TestIterate(t *testing.T, storeProvider StoreProvider) {
	t.Run("iterates over all keys", func(t *testing.T) {
		store := createStore(t, storeProvider)

		// Write some data
		_ = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			_ = writer.Put(stoabs.BytesKey(bytesKey), bytesValue)
			return writer.Put(stoabs.BytesKey(largerBytesKey), largerBytesValue)
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

		assert.Contains(t, keys, bytesKey)
		assert.Contains(t, keys, largerBytesKey)
		assert.Contains(t, values, bytesValue)
		assert.Contains(t, values, largerBytesValue)
	})

	t.Run("error", func(t *testing.T) {
		store := createStore(t, storeProvider)

		// Write some data otherwise shelf is empty and no error can be returned
		_ = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
			return writer.Put(stoabs.BytesKey(bytesKey), bytesValue)
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

func TestClose(t *testing.T, storeProvider StoreProvider) {
	t.Run("Close()", func(t *testing.T) {
		t.Run("close closed store", func(t *testing.T) {
			store := createStore(t, storeProvider)
			assert.NoError(t, store.Close(context.Background()))
			assert.NoError(t, store.Close(context.Background()))
		})
		t.Run("timeout", func(t *testing.T) {
			store := createStore(t, storeProvider)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := store.Close(ctx)
			assert.Equal(t, err, context.Canceled)
		})
	})
}

func createStore(t *testing.T, provider StoreProvider) stoabs.KVStore {
	store, err := provider(t)
	if !assert.NoError(t, err) {
		t.Fatalf("Unable to create store: %s", err)
	}
	t.Cleanup(func() {
		_ = store.Close(context.Background())
	})
	return store
}

// TODO: Write in other shelf with same key name, make sure they don't overwrite
