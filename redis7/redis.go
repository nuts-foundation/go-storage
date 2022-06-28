package redis7

import (
	"context"
	"github.com/go-redis/redis/v9"
	"github.com/nuts-foundation/go-stoabs"
	"strings"
)

var _ stoabs.KVStore = (*store)(nil)
var _ stoabs.ReadTx = (*tx)(nil)
var _ stoabs.WriteTx = (*tx)(nil)
var _ stoabs.Reader = (*shelf)(nil)
var _ stoabs.Writer = (*shelf)(nil)

func CreateRedisStore(opts *redis.Options) (stoabs.KVStore, error) {
	client := redis.NewClient(opts)
	// TODO: actually test the connection?
	return &store{client: client}, nil
}

type store struct {
	client *redis.Client
}

func (s store) Close(ctx context.Context) error {
	// TODO: Use context, wait for done (timeout)
	return s.client.Close()
}

func (s store) Write(fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	return fn(&tx{client: s.client})
}

func (s store) Read(fn func(stoabs.ReadTx) error) error {
	return fn(&tx{client: s.client})
}

func (s store) WriteShelf(shelfName string, fn func(stoabs.Writer) error) error {
	return fn(&shelf{name: shelfName, client: s.client})
}

func (s store) ReadShelf(shelfName string, fn func(stoabs.Reader) error) error {
	return fn(&shelf{name: shelfName, client: s.client})
}

type tx struct {
	client *redis.Client
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
	//TODO implement me
	panic("implement me")
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
	// TODO: Should context be passable?
	return s.client.Set(context.Background(), s.getRedisKey(key), value, 0).Err()
}

func (s shelf) Delete(key stoabs.Key) error {
	// TODO: Should context be passable?
	return s.client.Del(context.Background(), s.getRedisKey(key)).Err()
}

func (s shelf) Get(key stoabs.Key) ([]byte, error) {
	// TODO: Should context be passable?
	result, err := s.client.Get(context.Background(), s.getRedisKey(key)).Result()
	if err != redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return []byte(result), nil
}

func (s shelf) Iterate(callback stoabs.CallerFn) error {
	//TODO implement me
	panic("implement me")
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
