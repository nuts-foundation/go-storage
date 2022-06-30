package redis7

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
)

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
		mux:    &sync.Mutex{},
	}
	// TODO: Use options
	result.log = logrus.StandardLogger()
	return result, nil
}

type store struct {
	client *redis.Client
	log    *logrus.Logger
	mux    *sync.Mutex
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

func (s store) Write(fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	return fn(&tx{client: s.client, store: &s})
}

func (s store) Read(fn func(stoabs.ReadTx) error) error {
	return fn(&tx{client: s.client, store: &s})
}

func (s store) WriteShelf(shelfName string, fn func(stoabs.Writer) error) error {
	return fn(&shelf{name: shelfName, client: s.client})
}

func (s store) ReadShelf(shelfName string, fn func(stoabs.Reader) error) error {
	return fn(&shelf{name: shelfName, client: s.client})
}

type tx struct {
	client *redis.Client
	store  stoabs.KVStore
}

func (t tx) GetShelfWriter(shelfName string) (stoabs.Writer, error) {
	return &shelf{
		name:   shelfName,
		client: t.client,
	}, nil
}

func (t tx) GetShelfReader(shelfName string) (stoabs.Reader, error) {
	return &shelf{
		name:   shelfName,
		client: t.client,
	}, nil
}

func (t tx) Store() stoabs.KVStore {
	return t.store
}

func (t tx) Unwrap() interface{} {
	//TODO implement me
	panic("implement me")
}

type shelf struct {
	name   string
	client *redis.Client
}

func (s shelf) Put(key stoabs.Key, value []byte) error {
	return s.client.Set(context.TODO(), s.getRedisKey(key), value, 0).Err()
}

func (s shelf) Delete(key stoabs.Key) error {
	return s.client.Del(context.TODO(), s.getRedisKey(key)).Err()
}

func (s shelf) Get(key stoabs.Key) ([]byte, error) {
	result, err := s.client.Get(context.TODO(), s.getRedisKey(key)).Result()
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
		scanCmd := s.client.Scan(context.TODO(), cursor, s.name+"*", 100)
		keys, cursor, err = scanCmd.Result()
		if err != nil {
			return err
		}
		getCmd := s.client.MGet(context.Background(), keys...)
		values, err := getCmd.Result()
		if err != nil {
			return err
		}
		for i, key := range keys {
			if values[i] == nil {
				// Value does not exist (anymore), or not a string
				continue
			}
			err = callback(stoabs.BytesKey(key), []byte(values[i].(string)))
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
	panic("implement me")
}

func (s shelf) Stats() stoabs.ShelfStats {
	//TODO implement me
	panic("implement me")
}

func (s shelf) getRedisKey(key stoabs.Key) string {
	return strings.Join([]string{s.name, key.String()}, ".")
}
