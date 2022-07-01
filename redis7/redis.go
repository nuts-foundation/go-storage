package redis7

import (
	"context"
	"github.com/go-redis/redis/v9"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
)

const scanResultCount = 100

var _ stoabs.KVStore = (*store)(nil)
var _ stoabs.ReadTx = (*tx)(nil)
var _ stoabs.WriteTx = (*tx)(nil)
var _ stoabs.Reader = (*shelf)(nil)
var _ stoabs.Writer = (*shelf)(nil)

func CreateRedisStore(opts *redis.Options) (stoabs.KVStore, error) {
	client := redis.NewClient(opts)
	// TODO: actually test the connection?
	result := &store{
		client: client,
		mux:    &sync.RWMutex{},
	}
	// TODO: Use options
	result.log = logrus.StandardLogger()
	return result, nil
}

type store struct {
	client *redis.Client
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
	return nil
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
		scanCmd := s.reader.Scan(context.TODO(), cursor, s.name+"*", scanResultCount)
		keys, cursor, err = scanCmd.Result()
		if err != nil {
			return err
		}
		getCmd := s.reader.MGet(context.Background(), keys...)
		values, err := getCmd.Result()
		if err != nil {
			return err
		}
		for i, key := range keys {
			if values[i] == nil {
				// Value does not exist (anymore), or not a string
				continue
			}
			err = callback(fromRedisKey(s.name, stoabs.BytesKey(key)), []byte(values[i].(string)))
			if err != nil {
				// Callback returned an error, stop iterate and return it
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

func (s shelf) Range(from stoabs.Key, to stoabs.Key, callback stoabs.CallerFn) error {
	//TODO implement me
	//panic("implement me")
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

func fromRedisKey(shelfName string, key stoabs.Key) stoabs.Key {
	// TODO: Does string(key) work for all keys? Especially when some kind of guaranteed ordering is expected?
	// TODO: What is a good separator for shelf - key notation? Something that's highly unlikely to be used in a key (or maybe we should validate the keys)
	return stoabs.BytesKey(strings.TrimPrefix(string(key.Bytes()), shelfName+"."))
}
