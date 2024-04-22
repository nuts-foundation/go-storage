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
	"fmt"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

const resultCount = 1000

// lockExpiryOffset specifies how much time is added to the context deadline as expiry for TX locks
const lockExpiryOffset = 5 * time.Second

// pingAttempts specifies how many times a ping (Redis connection check) should be attempted.
const pingAttempts = 5

const pingTimeout = 3 * time.Second

// PingAttemptBackoff specifies how long the client should wait after a failed ping attempt.
var PingAttemptBackoff = 2 * time.Second

var _ stoabs.KVStore = (*store)(nil)
var _ stoabs.ReadTx = (*tx)(nil)
var _ stoabs.WriteTx = (*tx)(nil)
var _ stoabs.Reader = (*shelf)(nil)
var _ stoabs.Writer = (*shelf)(nil)

// CreateRedisStore connects to a Redis database server using the given options.
// The given prefix is added to each key, separated with a semicolon (:). When prefix is an empty string, it is ignored.
func CreateRedisStore(prefix string, clientOpts *redis.Options, opts ...stoabs.Option) (stoabs.KVStore, error) {
	client := redis.NewClient(clientOpts)
	return Wrap(prefix, client, opts...)
}

// Wrap can be used to use an already created Redis client as KVStore.
// This allows the application to use features supported by the Redis client library, but not by go-stoabs (e.g. Sentinel).
func Wrap(prefix string, client *redis.Client, opts ...stoabs.Option) (stoabs.KVStore, error) {
	cfg := stoabs.DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	result := &store{
		prefix: prefix,
		mux:    &sync.RWMutex{},
		cfg:    cfg,
	}

	result.log = cfg.Log

	var err error
	for i := 0; i < pingAttempts; i++ {
		result.log.Debugf("Checking connection to Redis database (attempt=%d/%d, address=%s)...", i+1, pingAttempts, client.Options().Addr)
		ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		_, err = client.Ping(ctx).Result()
		cancel()
		if err == nil {
			break
		}
		result.log.Warnf("Redis database connection check failed (attempt=%d/%d, address=%s): %s", i+1, pingAttempts, client.Options().Addr, err)
		time.Sleep(PingAttemptBackoff)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Redis database: %w", stoabs.DatabaseError(err))
	}
	result.log.Debug("Connection check successful")

	result.client = client
	result.rs = redsync.New(goredis.NewPool(client))

	return result, nil
}

type store struct {
	client *redis.Client
	rs     *redsync.Redsync
	log    *logrus.Logger
	mux    *sync.RWMutex
	// prefix contains a string that is prepended to each key, to simulate separate databases.
	// Redis doesn't supported named databases but only preconfigured, (numerically) indexed databases
	// which isn't very practical.
	prefix string
	cfg    stoabs.Config
}

func (s *store) Close(ctx context.Context) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.log.Debug("Closing Redis store")
	if s.client == nil {
		// already closed
		return nil
	}
	err := util.CallWithTimeout(ctx, s.client.Close, func() {
		s.log.Error("Closing of Redis client timed out")
	})
	s.client = nil
	if err != nil {
		return stoabs.DatabaseError(err)
	}
	return nil
}

func (s *store) Write(ctx context.Context, fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return s.doTX(ctx, func(ctx context.Context, writer redis.Pipeliner) error {
		return fn(&tx{writer: writer, reader: s.client, store: s, ctx: ctx})
	}, opts)
}

func (s *store) Read(ctx context.Context, fn func(stoabs.ReadTx) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return fn(&tx{reader: s.client, store: s, ctx: ctx})
}

func (s *store) WriteShelf(ctx context.Context, shelfName string, fn func(stoabs.Writer) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.doTX(ctx, func(ctx context.Context, tx redis.Pipeliner) error {
		return fn(s.getShelf(ctx, shelfName, tx, s.client))
	}, nil)
}

func (s *store) ReadShelf(ctx context.Context, shelfName string, fn func(stoabs.Reader) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return fn(s.getShelf(ctx, shelfName, nil, s.client))
}

func (s *store) getShelf(ctx context.Context, shelfName string, writer redis.Cmdable, reader redis.Cmdable) *shelf {
	return &shelf{
		name:   shelfName,
		prefix: s.prefix,
		writer: writer,
		reader: reader,
		store:  s,
		ctx:    ctx,
	}
}

func (s *store) doTX(ctx context.Context, fn func(ctx context.Context, tx redis.Pipeliner) error, opts []stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	// Make sure the transaction context has a deadline, to avoid hanging transactions and never-released locks
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, stoabs.DefaultTransactionTimeout)
		defer cancel()
	}

	unlock := func() {}

	// Obtain transaction-level write lock, if requested
	var txMutex *redsync.Mutex
	if (stoabs.WriteLockOption{}).Enabled(opts) {
		lockName := "lock_" + s.prefix
		s.log.Tracef("Acquiring Redis distributed lock (name=%s)", lockName)
		// Lock expires 5 seconds after transaction context expires
		txDeadline, _ := ctx.Deadline()
		lockExpiry := time.Until(txDeadline.Add(lockExpiryOffset))
		// Sub-context for lock acquisition
		lockCtx, lockCtxCancel := context.WithTimeout(ctx, s.cfg.LockAcquireTimeout)
		defer lockCtxCancel()
		// Acquire lock
		txMutex = s.rs.NewMutex(lockName, redsync.WithExpiry(lockExpiry))
		err := txMutex.LockContext(lockCtx)
		if err != nil {
			return fmt.Errorf("unable to obtain Redis transaction-level write lock: %w", stoabs.DatabaseError(err))
		}
		unlock = func() {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				s.log.Warnf("Not releasing Redis distributed lock, because the transaction context has expired and the server may still writing the data. Lock will expire automatically (name=%s,expiresIn=%s)", lockName, time.Until(txMutex.Until()))
				return
			}
			s.log.Tracef("Releasing Redis distributed lock (name=%s)", lockName)
			releaseLockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := txMutex.UnlockContext(releaseLockCtx)
			if err != nil {
				s.log.Errorf("Unable to release Redis transaction-level write lock: %s", err)
			}
		}
	}

	// Start transaction, retrieve/create shelf to operate on
	s.log.Tracef("Starting Redis transaction (TxPipeline)")
	pl := s.client.TxPipeline()

	// Perform TX action(s)
	appError := fn(ctx, pl)

	// Observe result, if application returned an error rollback TX
	if appError != nil {
		s.log.WithError(appError).Warn("Rolling back transaction due to application error")
		pl.Discard()
		unlock()
		stoabs.OnRollbackOption{}.Invoke(opts)
		return appError
	}

	s.log.Trace("Committing Redis transaction (TxPipeline)")

	// Before committing TX, check if TX didn't expire
	if ctx.Err() != nil {
		// TX lock expired
		pl.Discard()
		s.log.Error("Unable to commit Redis transaction, transaction timed out.")
		unlock()
		stoabs.OnRollbackOption{}.Invoke(opts)
		return stoabs.DatabaseError(ctx.Err())
	}

	// Everything looks OK, commit
	cmdErrs, err := pl.Exec(ctx)
	if err != nil {
		// Commit failed
		for _, cmdErr := range cmdErrs {
			s.log.WithError(cmdErr.Err()).Errorf("Redis pipeline command failed: %s", cmdErr.String())
		}
		unlock()
		stoabs.OnRollbackOption{}.Invoke(opts)
		return util.WrapError(stoabs.ErrCommitFailed, err)
	}

	// Success
	unlock()
	stoabs.AfterCommitOption{}.Invoke(opts)
	return nil
}

func (s *store) checkOpen() error {
	s.mux.RLock()
	defer s.mux.RUnlock()
	if s.client == nil {
		return stoabs.ErrStoreIsClosed
	}
	return nil
}

type tx struct {
	reader redis.Cmdable
	writer redis.Cmdable
	store  *store
	ctx    context.Context
}

func (t tx) GetShelfWriter(shelfName string) stoabs.Writer {
	return t.store.getShelf(t.ctx, shelfName, t.writer, t.reader)
}

func (t tx) GetShelfReader(shelfName string) stoabs.Reader {
	return t.store.getShelf(t.ctx, shelfName, nil, t.reader)
}

func (t tx) Store() stoabs.KVStore {
	return t.store
}

func (t tx) Unwrap() interface{} {
	return t.writer
}

type shelf struct {
	name   string
	prefix string
	reader redis.Cmdable
	// writer holds the Redis command writer for writing to the database.
	// It is separate from the reader because writes use a transactional Redis pipeline, which buffers the commands.
	// Buffering read commands would result in unexpected behaviour where the reads are only executed on transaction commit.
	writer redis.Cmdable
	store  *store
	ctx    context.Context
}

func (s shelf) Put(key stoabs.Key, value []byte) error {
	if err := s.store.checkOpen(); err != nil {
		return err
	}
	if err := s.writer.Set(s.ctx, s.toRedisKey(key), value, 0).Err(); err != nil {
		return stoabs.DatabaseError(err)
	}
	return nil
}

func (s shelf) PutTTL(key stoabs.Key, value []byte, duration time.Duration) error {
	if err := s.store.checkOpen(); err != nil {
		return err
	}
	if err := s.writer.Set(s.ctx, s.toRedisKey(key), value, duration).Err(); err != nil {
		return stoabs.DatabaseError(err)
	}
	return nil
}

func (s shelf) Delete(key stoabs.Key) error {
	if err := s.writer.Del(s.ctx, s.toRedisKey(key)).Err(); err != nil {
		return stoabs.DatabaseError(err)
	}
	return nil
}

func (s shelf) Empty() (bool, error) {
	// Redis has no stats, so we start an iterator and stop after n == 1
	var cursor uint64
	var err error
	var keys []string

	scanCmd := s.reader.Scan(s.ctx, cursor, s.toRedisKey(stoabs.BytesKey(""))+"*", int64(resultCount))
	keys, cursor, err = scanCmd.Result()
	if err != nil {
		return false, err
	}
	if len(keys) == 0 {
		return true, nil
	}

	return false, nil
}

func (s shelf) Get(key stoabs.Key) ([]byte, error) {
	result, err := s.reader.Get(s.ctx, s.toRedisKey(key)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, stoabs.ErrKeyNotFound
	} else if err != nil {
		return nil, stoabs.DatabaseError(err)
	}
	return []byte(result), nil
}

func (s shelf) Iterate(callback stoabs.CallerFn, keyType stoabs.Key) error {
	var cursor uint64
	var err error
	var keys []string
	for {
		scanCmd := s.reader.Scan(s.ctx, cursor, s.toRedisKey(stoabs.BytesKey(""))+"*", int64(resultCount))
		keys, cursor, err = scanCmd.Result()
		if err != nil {
			return stoabs.DatabaseError(err)
		}
		if len(keys) > 0 {
			_, err := s.visitKeys(keys, callback, keyType, false)
			if err != nil {
				return err
			}
		}
		if cursor == 0 {
			// Done
			break
		}
	}
	return nil
}

func (s shelf) Range(from stoabs.Key, to stoabs.Key, callback stoabs.CallerFn, stopAtNil bool) error {
	keys := make([]string, 0, resultCount)
	// Iterate from..to (start inclusive, end exclusive)
	var numKeys = 0
	for curr := from; !curr.Equals(to); curr = curr.Next() {
		// Potentially long-running operation, check context for cancellation
		if s.ctx.Err() != nil {
			return stoabs.DatabaseError(s.ctx.Err())
		}
		if curr.Equals(to) {
			// Reached end (exclusive)
			break
		}
		keys = append(keys, s.toRedisKey(curr))
		// We don't want to perform requests that are really large, so we limit it at page size
		if numKeys >= resultCount {
			proceed, err := s.visitKeys(keys, callback, from, stopAtNil)
			if err != nil || !proceed {
				return err
			}
			keys = make([]string, 0, resultCount)
			numKeys = 0
		} else {
			numKeys++
		}
	}
	_, err := s.visitKeys(keys, callback, from, stopAtNil)
	return err
}

// visitKeys retrieves the values of the given keys and invokes the callback with each key and value.
// It returns a bool indicating whether subsequent calls to visitKeys (with larger keys) should be attempted.
// Behavior when encountering a non-existing key depends on stopAtNil:
// - If stopAtNil is true, it stops processing keys and returns false (no further calls to visitKeys should be made).
// - If stopAtNil is false, it proceeds with the next key.
// If an error occurs it also returns false.
func (s shelf) visitKeys(keys []string, callback stoabs.CallerFn, keyType stoabs.Key, stopAtNil bool) (bool, error) {
	values, err := s.reader.MGet(s.ctx, keys...).Result()
	if err != nil {
		return false, stoabs.DatabaseError(err)
	}
	for i, value := range values {
		if values[i] == nil {
			// Value does not exist (anymore), or not a string
			if stopAtNil {
				return false, nil
			}
			continue
		}
		key, err := s.fromRedisKey(keys[i], keyType)
		if err != nil {
			return false, err
		}
		err = callback(key, []byte(value.(string)))
		if err != nil {
			// Callback returned an error, stop iterate and return it
			return false, err
		}
	}
	return true, nil
}

func (s shelf) Stats() stoabs.ShelfStats {
	return stoabs.ShelfStats{
		NumEntries: 0,
		ShelfSize:  0,
	}
}

func (s shelf) toRedisKey(key stoabs.Key) string {
	result := s.name + "." + key.String()
	if len(s.prefix) > 0 {
		result = s.prefix + ":" + result
	}
	return result
}

func (s shelf) fromRedisKey(key string, keyType stoabs.Key) (stoabs.Key, error) {
	// returned errors are the result of invalid input, hence not wrapped in ErrDatabase
	if len(s.prefix) > 0 {
		dbPrefix := s.prefix + ":"
		if !strings.HasPrefix(key, dbPrefix) {
			return nil, fmt.Errorf("unexpected/missing database name in Redis key (expected=%s,key=%s)", dbPrefix, key)
		}
		key = strings.TrimPrefix(key, dbPrefix)
	}
	shelfPrefix := s.name + "."
	if !strings.HasPrefix(key, shelfPrefix) {
		return nil, fmt.Errorf("unexpected/missing shelf name in Redis key (expected=%s,key=%s)", shelfPrefix, key)
	}
	return keyType.FromString(strings.TrimPrefix(key, shelfPrefix))
}
