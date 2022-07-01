package redis7

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/kvtests"
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

	kvtests.TestClose(t, provider)
	kvtests.TestIterate(t, provider)
	kvtests.TestReadingAndWriting(t, provider)
	// TODO: Did not find out how to efficiently calculate stats for Redis.
	// kvtests.TestStats(t, provider)
	kvtests.TestRange(t, provider)
}
