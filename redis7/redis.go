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
)

const resultCount = 1000

var _ stoabs.KVStore = (*store)(nil)
var _ stoabs.ReadTx = (*tx)(nil)
var _ stoabs.WriteTx = (*tx)(nil)
var _ stoabs.Reader = (*shelf)(nil)
var _ stoabs.Writer = (*shelf)(nil)

// CreateRedisStore connects to a Redis database server using the given options.
// The given prefix is added to each key, separated with a semicolon (:). When prefix is an empty string, it is ignored.
func CreateRedisStore(prefix string, clientOpts *redis.Options, opts ...stoabs.Option) (stoabs.KVStore, error) {
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
	return err
}

func (s *store) Write(fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return s.doTX(func(writer redis.Pipeliner) error {
		return fn(&tx{writer: writer, reader: s.client, store: s})
	}, opts)
}

func (s *store) Read(fn func(stoabs.ReadTx) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return fn(&tx{reader: s.client, store: s})
}

func (s *store) WriteShelf(shelfName string, fn func(stoabs.Writer) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.doTX(func(tx redis.Pipeliner) error {
		return fn(s.getShelf(shelfName, tx, s.client))
	}, nil)
}

func (s *store) ReadShelf(shelfName string, fn func(stoabs.Reader) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return fn(s.getShelf(shelfName, nil, s.client))
}

func (s *store) getShelf(shelfName string, writer redis.Cmdable, reader redis.Cmdable) *shelf {
	return &shelf{
		name:   shelfName,
		prefix: s.prefix,
		writer: writer,
		reader: reader,
		store:  s,
	}
}

func (s *store) doTX(fn func(tx redis.Pipeliner) error, optsSlice []stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	opts := stoabs.TxOptions(optsSlice)

	// Obtain transaction-level write lock, if requested
	var txMutex *redsync.Mutex
	if opts.RequestsWriteLock() {
		lockName := "lock_" + s.prefix
		s.log.Tracef("Acquiring Redis distributed lock (name=%s)", lockName)
		txMutex = s.rs.NewMutex(lockName)
		err := txMutex.Lock()
		if err != nil {
			return fmt.Errorf("unable to obtain Redis transaction-level write lock: %w", err)
		}
		defer func(txMutex *redsync.Mutex, log *logrus.Logger) {
			s.log.Tracef("Releasing Redis distributed lock (name=%s)", lockName)
			_, err := txMutex.Unlock()
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
		cmdErrs, err := pl.Exec(context.TODO())
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
}

func (t tx) GetShelfWriter(shelfName string) (stoabs.Writer, error) {
	return t.store.getShelf(shelfName, t.writer, t.reader), nil
}

func (t tx) GetShelfReader(shelfName string) stoabs.Reader {
	return t.store.getShelf(shelfName, nil, t.reader)
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
}

func (s shelf) Put(key stoabs.Key, value []byte) error {
	if err := s.store.checkOpen(); err != nil {
		return err
	}
	return s.writer.Set(context.TODO(), s.toRedisKey(key), value, 0).Err()
}

func (s shelf) Delete(key stoabs.Key) error {
	return s.writer.Del(context.TODO(), s.toRedisKey(key)).Err()
}

func (s shelf) Get(key stoabs.Key) ([]byte, error) {
	result, err := s.reader.Get(context.TODO(), s.toRedisKey(key)).Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return []byte(result), nil
}

func (s shelf) Iterate(callback stoabs.CallerFn) error {
	var cursor uint64
	var err error
	var keys []string
	for {
		scanCmd := s.reader.Scan(context.TODO(), cursor, s.toRedisKey(stoabs.BytesKey("*")), int64(resultCount))
		keys, cursor, err = scanCmd.Result()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			// Nothing to iterate over
			return nil
		}
		_, err := s.visitKeys(keys, callback, false)
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
		if curr.Equals(to) {
			// Reached end (exclusive)
			break
		}
		keys = append(keys, s.toRedisKey(curr))
		// We don't want to perform requests that are really large, so we limit it at page size
		if numKeys >= resultCount {
			proceed, err := s.visitKeys(keys, callback, stopAtNil)
			if err != nil || !proceed {
				return err
			}
			keys = make([]string, 0, resultCount)
			numKeys = 0
		} else {
			numKeys++
		}
	}
	_, err := s.visitKeys(keys, callback, stopAtNil)
	return err
}

func (s shelf) visitKeys(keys []string, callback stoabs.CallerFn, stopAtNil bool) (bool, error) {
	values, err := s.reader.MGet(context.TODO(), keys...).Result()
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
		err := callback(s.fromRedisKey(keys[i]), []byte(value.(string)))
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
	// TODO: Does string(key) work for all keys? Especially when some kind of guaranteed ordering is expected?
	// TODO: What is a good separator for shelf - key notation? Something that's highly unlikely to be used in a key (or maybe we should validate the keys)
	result := s.name + "." + string(key.Bytes())
	if len(s.prefix) > 0 {
		result = s.prefix + ":" + result
	}
	return result
}

func (s shelf) fromRedisKey(key string) stoabs.Key {
	// TODO: Does string(key) work for all keys? Especially when some kind of guaranteed ordering is expected?
	// TODO: What is a good separator for shelf - key notation? Something that's highly unlikely to be used in a key (or maybe we should validate the keys)
	if len(s.prefix) > 0 {
		key = strings.TrimPrefix(key, s.prefix+":")
	}
	return stoabs.BytesKey(strings.TrimPrefix(key, s.name+"."))
}
