package bbolt

import (
	"context"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

func Test_adapter_CreateStore(t *testing.T) {
	t.Run("default mode", func(t *testing.T) {
		path := path.Join(util.TestDirectory(t), "bbolt.db")
		store, err := CreateAdapter().CreateStore(map[string]interface{}{"path": path})
		defer store.Close(context.Background())

		if !assert.NoError(t, err) {
			return
		}
		assert.NotNil(t, store)

		store.Close(context.Background())

		info, err := os.Lstat(path)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, defaultFileMode, info.Mode())
	})

}
