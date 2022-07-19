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
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

const resultCount = 1000

var _ stoabs.KVStore = (*store)(nil)
var _ stoabs.ReadTx = (*tx)(nil)
var _ stoabs.WriteTx = (*tx)(nil)
var _ stoabs.Reader = (*shelf)(nil)
var _ stoabs.Writer = (*shelf)(nil)

// CreateRedisStore connects to a Redis database server using the given options.
// The given prefix is added to each key, separated with a semicolon (:). When prefix is an empty string, it is ignored.
func CreateRedisStore(prefix string, clientOpts *redis.Options, lockOpts []redsync.Option, opts ...stoabs.Option) (stoabs.KVStore, error) {
	cfg := stoabs.Config{}
	for _, opt := range opts {
		opt(&cfg)
	}

	result := &store{
		prefix: prefix,
		mux:    &sync.RWMutex{},
		cfg:    cfg,
	}

	result.log = cfg.Log
	if result.log == nil {
		result.log = logrus.StandardLogger()
	}

	client := redis.NewClient(clientOpts)

	result.log.Debugf("Checking connection to Redis database (address=%s)...", client.Options().Addr)
	_, err := client.Ping(context.TODO()).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Redis database: %w", err)
	}
	result.log.Debug("Connection check successful")

	result.client = client
	result.rs = redsync.New(goredis.NewPool(client))
	result.rsOpts = append([]redsync.Option{}, lockOpts...)

	return result, nil
}

type store struct {
	client *redis.Client
	rs     *redsync.Redsync
	rsOpts []redsync.Option
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
	return err
}

func (s *store) Write(ctx context.Context, fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return s.doTX(ctx, func(writer redis.Pipeliner) error {
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
	return s.doTX(ctx, func(tx redis.Pipeliner) error {
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

func (s *store) doTX(ctx context.Context, fn func(tx redis.Pipeliner) error, optsSlice []stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	opts := stoabs.TxOptions(optsSlice)

	// Obtain transaction-level write lock, if requested
	var txMutex *redsync.Mutex
	if opts.RequestsWriteLock() {
		lockName := "lock_" + s.prefix
		s.log.Tracef("Acquiring Redis distributed lock (name=%s)", lockName)
		txMutex = s.rs.NewMutex(lockName, s.rsOpts...)
		err := txMutex.LockContext(ctx)
		if err != nil {
			return fmt.Errorf("unable to obtain Redis transaction-level write lock: %w", err)
		}
		defer func(txMutex *redsync.Mutex, log *logrus.Logger) {
			s.log.Tracef("Releasing Redis distributed lock (name=%s)", lockName)
			_, err := txMutex.UnlockContext(ctx)
			if err != nil {
				log.Errorf("Unable to release Redis transaction-level write lock: %s", err)
			}
		}(txMutex, s.log)
	}

	// Start transaction, retrieve/create shelf to operate on
	s.log.Tracef("Starting Redis transaction (TxPipeline)")
	pl := s.client.TxPipeline()

	// Perform TX action(s)
	appError := fn(pl)

	// Observe result, commit/rollback
	if appError == nil {
		s.log.Trace("Committing Redis transaction (TxPipeline)")
		// We need to check whether we still hold the lock, and if we have enough time to commit the TX.
		// If there's too little time, we need to extend the lock.
		if txMutex != nil && !time.Now().Before(txMutex.Until()) {
			// TX lock expired
			pl.Discard()
			s.log.Error("Unable to commit Redis transaction, write lock expired.")
			opts.InvokeOnRollback()
			return stoabs.ErrLockExpired
		} else if txMutex != nil && time.Now().Sub(txMutex.Until()) < time.Second {
			// TX lock expires soon. Extend it, so we have time to commit.
			extended, err := txMutex.ExtendContext(ctx)
			if !extended || err != nil {
				s.log.Errorf("Unable to extend Redis transaction write lock: %s", err)
			}
			s.log.Debug("Extended Redis transaction write lock to ensure proper commit.")
		}
		cmdErrs, err := pl.Exec(ctx)

		if err != nil {
			for _, cmdErr := range cmdErrs {
				s.log.WithError(cmdErr.Err()).Errorf("Redis pipeline command failed: %s", cmdErr.String())
			}
			opts.InvokeOnRollback()
			return stoabs.ErrCommitFailed
		}
		opts.InvokeAfterCommit()
	} else {
		s.log.WithError(appError).Warn("Rolling back transaction due to application error")
		pl.Discard()
		opts.InvokeOnRollback()
		return appError
	}

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

func (t tx) GetShelfWriter(shelfName string) (stoabs.Writer, error) {
	return t.store.getShelf(t.ctx, shelfName, t.writer, t.reader), nil
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
	return s.writer.Set(s.ctx, s.toRedisKey(key), value, 0).Err()
}

func (s shelf) Delete(key stoabs.Key) error {
	return s.writer.Del(s.ctx, s.toRedisKey(key)).Err()
}

func (s shelf) Get(key stoabs.Key) ([]byte, error) {
	result, err := s.reader.Get(s.ctx, s.toRedisKey(key)).Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
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
			return err
		}
		if len(keys) == 0 {
			// Nothing to iterate over
			return nil
		}
		_, err := s.visitKeys(keys, callback, keyType, false)
		if err != nil {
			return err
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
		if s.ctx.Err() != nil {
			return s.ctx.Err()
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

func (s shelf) visitKeys(keys []string, callback stoabs.CallerFn, keyType stoabs.Key, stopAtNil bool) (bool, error) {
	values, err := s.reader.MGet(s.ctx, keys...).Result()
	if err != nil {
		return false, err
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
			return true, err
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
