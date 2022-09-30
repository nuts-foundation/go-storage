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

package redis7

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v9"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/kvtests"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRedis(t *testing.T) {
	runTests := func(t *testing.T, provider kvtests.StoreProvider) {
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

	t.Run("with database prefix", func(t *testing.T) {
		runTests(t, func(t *testing.T) (stoabs.KVStore, error) {
			s := miniredis.RunT(t)
			t.Cleanup(func() {
				s.Close()
			})
			return CreateRedisStore("db", &redis.Options{
				Addr: s.Addr(),
			})
		})
	})
	t.Run("without database prefix", func(t *testing.T) {
		runTests(t, func(t *testing.T) (stoabs.KVStore, error) {
			s := miniredis.RunT(t)
			t.Cleanup(func() {
				s.Close()
			})
			return CreateRedisStore("", &redis.Options{
				Addr: s.Addr(),
			})
		})
	})

	t.Run("context deadline is set, if not provided", func(t *testing.T) {
		s := miniredis.RunT(t)
		t.Cleanup(func() {
			s.Close()
		})
		redisStore, _ := CreateRedisStore("db", &redis.Options{
			Addr: s.Addr(),
		})
		defer redisStore.Close(context.Background())

		var deadline time.Time
		var set bool
		_ = redisStore.Write(context.Background(), func(tx stoabs.WriteTx) error {
			writer, _ := tx.GetShelfWriter("foo")
			deadline, set = writer.(*shelf).ctx.Deadline()
			return nil
		}, stoabs.WithWriteLock())

		assert.True(t, set)
		assert.NotEmpty(t, deadline)
	})
}

func TestCreateRedisStore(t *testing.T) {
	t.Run("unable to connect", func(t *testing.T) {
		PingAttemptBackoff = 100 * time.Millisecond // speed up test
		startTime := time.Now()

		actual, err := CreateRedisStore("", &redis.Options{Addr: "localhost:9889"})
		assert.ErrorContains(t, err, "unable to connect to Redis database")
		assert.Nil(t, actual)

		// Assert time took at least pingAttempts * pingAttemptBackoff
		assert.GreaterOrEqual(t, time.Since(startTime), pingAttempts*PingAttemptBackoff)
	})
}
