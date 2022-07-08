package redis7

import (
	"bytes"
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
func CreateRedisStore(clientOpts *redis.Options, opts ...stoabs.Option) (stoabs.KVStore, error) {
	cfg := stoabs.Config{}
	for _, opt := range opts {
		opt(&cfg)
	}

	client := redis.NewClient(clientOpts)

	_, err := client.Ping(context.TODO()).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Redis database: %w", err)
	}

	result := &store{
		client: client,
		rs:     redsync.New(goredis.NewPool(client)),
		mux:    &sync.RWMutex{},
	}

	result.log = cfg.Log
	if result.log == nil {
		result.log = logrus.StandardLogger()
	}
	return result, nil
}

type store struct {
	client *redis.Client
	rs     *redsync.Redsync
	log    *logrus.Logger
	mux    *sync.RWMutex
}

func (s *store) Close(ctx context.Context) error {
	s.mux.Lock()
	defer s.mux.Unlock()
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

	reader, err := s.getShelfReader(shelfName)
	if err != nil {
		return err
	}
	if reader == nil {
		return nil
	}
	return fn(s.getShelf(shelfName, nil, s.client))
}

func (s *store) getShelf(shelfName string, writer redis.Cmdable, reader redis.Cmdable) *shelf {
	return &shelf{
		name:   shelfName,
		writer: writer,
		reader: reader,
		store:  s,
	}
}

func (s store) getShelfReader(shelfName string) (stoabs.Reader, error) {
	// TODO: Is this too slow? Should we change the API as to just return a Reader, even when the shelf does not exist?
	scanCmd := s.client.Scan(context.TODO(), 0, toRedisKey(shelfName, stoabs.BytesKey("*")), 1)
	keys, _, err := scanCmd.Result()
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// Shelf does not exist
		return nil, nil
	}
	return s.getShelf(shelfName, nil, s.client), nil
}

func (s *store) doTX(fn func(tx redis.Pipeliner) error, optsSlice []stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	opts := stoabs.TxOptions(optsSlice)

	// Obtain transaction-level write lock, if requested
	var txMutex *redsync.Mutex
	if opts.RequestsWriteLock() {
		println("locking")
		txMutex = s.rs.NewMutex("global")
		err := txMutex.Lock()
		if err != nil {
			return fmt.Errorf("unable to obtain Redis transaction-level write lock: %w", err)
		}
		defer func(txMutex *redsync.Mutex, log *logrus.Logger) {
			_, err := txMutex.Unlock()
			if err != nil {
				log.Errorf("Unable to release Redis transaction-level write lock: %s", err)
			}
		}(txMutex, s.log)
	}

	// Start transaction, retrieve/create shelf to operate on
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

func (t tx) GetShelfReader(shelfName string) (stoabs.Reader, error) {
	reader, err := t.store.getShelfReader(shelfName)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, nil
	}
	return reader, nil
}

func (t tx) Store() stoabs.KVStore {
	return t.store
}

func (t tx) Unwrap() interface{} {
	return t.writer
}

type shelf struct {
	name   string
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
	return s.writer.Set(context.TODO(), toRedisKey(s.name, key), value, 0).Err()
}

func (s shelf) Delete(key stoabs.Key) error {
	return s.writer.Del(context.TODO(), toRedisKey(s.name, key)).Err()
}

func (s shelf) Get(key stoabs.Key) ([]byte, error) {
	result, err := s.reader.Get(context.TODO(), toRedisKey(s.name, key)).Result()
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
		scanCmd := s.reader.Scan(context.TODO(), cursor, s.name+"*", resultCount)
		keys, cursor, err = scanCmd.Result()
		if err != nil {
			return err
		}
		err := s.visitKeys(keys, callback)
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

func (s shelf) Range(from stoabs.Key, to stoabs.Key, callback stoabs.CallerFn) error {
	keys := make([]string, 0, resultCount)
	// Iterate from..to (start inclusive, end exclusive)
	var numKeys = 0
	for curr := from; bytes.Compare(curr.Bytes(), to.Bytes()) == -1; curr = curr.Next() {
		keys = append(keys, toRedisKey(s.name, curr))
		// We don't want to perform requests that are really large, so we limit it at resultCount keys
		if numKeys >= resultCount {
			err := s.visitKeys(keys, callback)
			if err != nil {
				return err
			}
			keys = make([]string, 0, resultCount)
			numKeys = 0
		} else {
			numKeys++
		}
	}
	return s.visitKeys(keys, callback)
}

func (s shelf) visitKeys(keys []string, callback stoabs.CallerFn) error {
	values, err := s.reader.MGet(context.TODO(), keys...).Result()
	if err != nil {
		return err
	}
	for i, value := range values {
		if values[i] == nil {
			// Value does not exist (anymore), or not a string
			continue
		}
		err := callback(fromRedisKey(s.name, keys[i]), []byte(value.(string)))
		if err != nil {
			// Callback returned an error, stop iterate and return it
			return err
		}
	}
	return nil
}

func (s shelf) Stats() stoabs.ShelfStats {
	return stoabs.ShelfStats{
		NumEntries: 0,
		ShelfSize:  0,
	}
}

func toRedisKey(shelfName string, key stoabs.Key) string {
	// TODO: Does string(key) work for all keys? Especially when some kind of guaranteed ordering is expected?
	// TODO: What is a good separator for shelf - key notation? Something that's highly unlikely to be used in a key (or maybe we should validate the keys)
	return strings.Join([]string{shelfName, string(key.Bytes())}, ".")
}

func fromRedisKey(shelfName string, key string) stoabs.Key {
	// TODO: Does string(key) work for all keys? Especially when some kind of guaranteed ordering is expected?
	// TODO: What is a good separator for shelf - key notation? Something that's highly unlikely to be used in a key (or maybe we should validate the keys)
	return stoabs.BytesKey(strings.TrimPrefix(key, shelfName+"."))
}
