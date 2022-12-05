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
	"errors"
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
			writer := tx.GetShelfWriter("foo")
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

func TestRedis_Close(t *testing.T) {
	ctx := context.Background()
	var bytesKey = stoabs.BytesKey([]byte{1, 2, 3})
	var bytesValue = bytesKey.Next().Bytes()
	_, store := NewTestStore(t)

	t.Run("Close()", func(t *testing.T) {
		t.Run("write to closed store", func(t *testing.T) {
			assert.NoError(t, store.Close(context.Background()))
			err := store.WriteShelf(ctx, "shelf", func(writer stoabs.Writer) error {
				return writer.Put(bytesKey, bytesValue)
			})
			assert.Equal(t, stoabs.ErrStoreIsClosed, err)
		})
	})
}

func NewTestStore(t *testing.T) (*miniredis.Miniredis, store) {
	mr := miniredis.RunT(t)
	t.Cleanup(func() {
		mr.Close()
	})
	s, err := CreateRedisStore("db", &redis.Options{
		Addr: mr.Addr(),
	})
	if !assert.NoError(t, err) {
		t.Fatal(err)
	}
	return mr, *s.(*store)
}

func TestStore_ErrDatabase(t *testing.T) {
	throwDBError := func(t *testing.T, fn func(writer stoabs.Writer) error) {
		t.Run("contains ErrDatabase and mock error", func(t *testing.T) {
			mock, store := NewTestStore(t)
			mock.SetError("db error")

			err := store.WriteShelf(context.Background(), "shelf", fn)

			assert.ErrorIs(t, err, stoabs.ErrDatabase{})
			assert.Contains(t, err.Error(), "db error")
		})
	}

	t.Run("Get()", func(t *testing.T) {
		throwDBError(t, func(writer stoabs.Writer) error {
			_, err := writer.Get(stoabs.NewHashKey([32]byte{}))
			return err
		})
	})
	t.Run("Put()", func(t *testing.T) {
		throwDBError(t, func(writer stoabs.Writer) error {
			return writer.Put(stoabs.NewHashKey([32]byte{}), []byte{1})
		})
	})
	t.Run("Delete()", func(t *testing.T) {
		throwDBError(t, func(writer stoabs.Writer) error {
			return writer.Delete(stoabs.NewHashKey([32]byte{}))
		})
	})
	t.Run("Close()", func(t *testing.T) {
		_, store := NewTestStore(t)
		// mock.SetError doesn't work for Close
		_ = store.client.Close()

		err := store.Close(context.Background())

		assert.ErrorIs(t, err, stoabs.ErrDatabase{})
	})
	t.Run("user err", func(t *testing.T) {
		_, store := NewTestStore(t)
		expected := errors.New("user error")

		actual := store.ReadShelf(context.Background(), "shelf", func(reader stoabs.Reader) error {
			return expected
		})

		assert.Equal(t, actual, expected)
		assert.NotErrorIs(t, actual, stoabs.ErrDatabase{})
	})
}
