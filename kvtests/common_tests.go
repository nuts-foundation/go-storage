package kvtests

import (
	"context"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/stretchr/testify/assert"
	"testing"
)

var key = []byte{1, 2, 3}
var value = []byte{4, 5, 6}

const shelf = "test"

func TestReadingAndWriting(t *testing.T, storeProvider func() (stoabs.KVStore, error)) {
	t.Helper()
	t.Run("write, then read", func(t *testing.T) {
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
}

// TODO: Write in other shelf with same key name, make sure they don't overwrite
