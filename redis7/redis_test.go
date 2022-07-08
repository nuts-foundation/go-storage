package redis7

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v9"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/kvtests"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRedis(t *testing.T) {
	provider := func(t *testing.T) (stoabs.KVStore, error) {
		s := miniredis.RunT(t)
		t.Cleanup(func() {
			s.Close()
		})
		return CreateRedisStore(&redis.Options{
			Addr: s.Addr(),
		})
	}

	kvtests.TestReadingAndWriting(t, provider)
	kvtests.TestRange(t, provider)
	kvtests.TestIterate(t, provider)
	kvtests.TestClose(t, provider)
	kvtests.TestDelete(t, provider)
	// TODO: Did not find out how to efficiently calculate stats for Redis.
	// kvtests.TestStats(t, provider)
	kvtests.TestWriteTransactions(t, provider)
	kvtests.TestTransactionWriteLock(t, provider)
}

func TestCreateRedisStore(t *testing.T) {
	t.Run("unable to connect", func(t *testing.T) {
		actual, err := CreateRedisStore(&redis.Options{Addr: "localhost:9889"})
		assert.ErrorContains(t, err, "unable to connect to Redis database")
		assert.Nil(t, actual)
	})
}
