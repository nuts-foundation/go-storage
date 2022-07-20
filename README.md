# Golang Storage Abstraction (go-stoabs)

## BBolt

## Redis

When creating a Redis `KVStore` it tests the connection using Redis' `PING` command.

Due to the simple API of the library, the Redis adapter only supports reading/writing byte arrays.
The behavior when reading any other Redis type (e.g. a list or set) is undefined.

### Transaction Isolation

Redis doesn't have actual transactions, so this library simulates them by using the `MULTI`/`EXEC`/`DISCARD` commands.
That way all commands are guaranteed to be executed atomically.

As a consequence, a value that hasn't been committed yet can't be read. In other words, don't try to read a value from a
key that was written to in the same transaction.
Subsequently, changes from other writers (from the same process or remote) are reflected immediately in the current
transaction: if a key is read twice, there's no guarantee the returned value will be equal.

If the application requires exclusive write access to a store it can lock the database, to assert there are no other
active writers:

```golang
store.Write(func (tx stoabs.WriteTx) error { 
	// do something with tx
}, stoabs.WithWriteLock())
```

The lock is released when the transaction is committed or rolled back.
The lock is subject to the prefix (`CreateRedisStore(prefix string, ...)`) the store was created with, meaning other stores with the same prefix will have the same lock.

Redis locks are implemented using (Redsync)[https://github.com/go-redsync/redsync].

### Unsupported features

* Clustering

