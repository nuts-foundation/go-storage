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
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v9"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/kvtests"
	"github.com/stretchr/testify/assert"
	"testing"
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
}

func TestCreateRedisStore(t *testing.T) {
	t.Run("unable to connect", func(t *testing.T) {
		actual, err := CreateRedisStore("", &redis.Options{Addr: "localhost:9889"})
		assert.ErrorContains(t, err, "unable to connect to Redis database")
		assert.Nil(t, actual)
	})
}
