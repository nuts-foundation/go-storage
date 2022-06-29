package kvtests

import (
	"context"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/stretchr/testify/assert"
	"testing"
)

var bytesKey = []byte{1, 2, 3}
var bytesValue = []byte{4, 5, 6}

const stringKey = "message"
const stringValue = "Hello, World!"

const shelf = "test"

func TestReadingAndWriting(t *testing.T, storeProvider func() (stoabs.KVStore, error)) {
	t.Helper()
	t.Run("write, then read (string)", func(t *testing.T) {
		store, err := storeProvider()
		if !assert.NoError(t, err) {
			return
		}
		defer store.Close(context.Background())

		err = store.Write(func(tx stoabs.WriteTx) error {
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
		store, err := storeProvider()
		if !assert.NoError(t, err) {
			return
		}
		defer store.Close(context.Background())

		called := false
		err = store.ReadShelf(shelf, func(reader stoabs.Reader) error {
			called = true
			return nil
		})

		assert.NoError(t, err)
		assert.False(t, called)
	})
	t.Run("read non-existing key", func(t *testing.T) {
		store, err := storeProvider()
		if !assert.NoError(t, err) {
			return
		}
		defer store.Close(context.Background())

		err = store.WriteShelf(shelf, func(writer stoabs.Writer) error {
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
}

// TODO: Write in other shelf with same key name, make sure they don't overwrite
