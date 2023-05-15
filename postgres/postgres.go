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

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/util"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

const resultCount = 20

// pingAttempts specifies how many times a ping (Redis connection check) should be attempted.
const pingAttempts = 5

// PingAttemptBackoff specifies how long the client should wait after a failed ping attempt.
var PingAttemptBackoff = 2 * time.Second

var _ stoabs.KVStore = (*store)(nil)
var _ stoabs.ReadTx = (*tx)(nil)
var _ stoabs.WriteTx = (*tx)(nil)
var _ stoabs.Reader = (*shelf)(nil)
var _ stoabs.Writer = (*shelf)(nil)

type queryable interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// CreatePostgresStore connects to a Postgres database server using the given options.
func CreatePostgresStore(db *sql.DB, opts ...stoabs.Option) (stoabs.KVStore, error) {
	result := store{
		db:             db,
		log:            logrus.New(),
		mux:            &sync.RWMutex{},
		createdShelves: new(sync.Map),
	}
	return &result, nil
}

type store struct {
	db             *sql.DB
	log            *logrus.Logger
	mux            *sync.RWMutex
	createdShelves *sync.Map
}

func (s *store) Close(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return stoabs.DatabaseError(err)
	}
	return nil
}

func (s *store) Write(ctx context.Context, fn func(stoabs.WriteTx) error, opts ...stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return s.doTX(ctx, func(ctx context.Context, sqlTx *sql.Tx) error {
		return fn(&tx{reader: sqlTx, writer: sqlTx, store: s, ctx: ctx})
	}, opts)
}

func (s *store) Read(ctx context.Context, fn func(stoabs.ReadTx) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return fn(&tx{reader: s.db, store: s, ctx: ctx})
}

func (s *store) WriteShelf(ctx context.Context, shelfName string, fn func(stoabs.Writer) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.doTX(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return fn(s.getShelf(ctx, shelfName, tx, tx))
	}, nil)
}

func (s *store) ReadShelf(ctx context.Context, shelfName string, fn func(stoabs.Reader) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return fn(s.getShelf(ctx, shelfName, s.db, nil))
}

func (s *store) getShelf(ctx context.Context, shelfName string, reader queryable, writer *sql.Tx) *shelf {
	result := &shelf{
		name:      shelfName,
		tableName: s.tableName(shelfName),
		writer:    writer,
		reader:    reader,
		store:     s,
		ctx:       ctx,
	}
	if err := s.createShelf(shelfName); err != nil {
		logrus.WithError(err).Error("Failed to create table for shelf, all operations will fail!")
	}
	return result
}

func (s *store) doTX(ctx context.Context, fn func(ctx context.Context, sqlTx *sql.Tx) error, opts []stoabs.TxOption) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	// TODO: Store-level locking (bad idea?)
	sqlTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return stoabs.DatabaseError(err)
	}

	// Perform TX action(s)
	appError := fn(ctx, sqlTx)

	// Observe result, if application returned an error rollback TX
	if appError != nil {
		s.log.WithError(appError).Warn("Rolling back transaction due to application error")
		err = sqlTx.Rollback()
		if err != nil {
			s.log.WithError(err).Warn("Transaction rollback failed")
		}
		stoabs.OnRollbackOption{}.Invoke(opts)
		return appError
	}

	// Everything looks OK, commit
	err = sqlTx.Commit()
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return util.WrapError(stoabs.ErrCommitFailed, ctx.Err())
		}
		s.log.WithError(err).Error("SQL transaction commit failed")
		stoabs.OnRollbackOption{}.Invoke(opts)
		return util.WrapError(stoabs.ErrCommitFailed, err)
	}

	// Success
	stoabs.AfterCommitOption{}.Invoke(opts)
	return nil
}

func (s *store) checkOpen() error {
	s.mux.RLock()
	defer s.mux.RUnlock()
	if s.db == nil {
		return stoabs.ErrStoreIsClosed
	}
	return nil
}

func (s *store) tableName(shelf string) string {
	shelf = strings.ReplaceAll(shelf, ".", "_")
	return pq.QuoteIdentifier("kv_" + shelf)
}

func (s *store) createShelf(shelf string) error {
	// Cache names of created shelves to avoid unnecessary DDL queries
	_, exists := s.createdShelves.Load(shelf)
	if exists {
		return nil
	}
	tableName := s.tableName(shelf)
	_, err := s.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    key BYTEA PRIMARY KEY NOT NULL,
    value BYTEA NOT NULL
    )`, tableName))
	if err != nil {
		return fmt.Errorf("failed to create table (%s): %w", tableName, err)
	}
	s.createdShelves.Store(shelf, true)
	return nil
}

type tx struct {
	writer *sql.Tx
	reader queryable
	store  *store
	ctx    context.Context
}

func (t tx) GetShelfWriter(shelfName string) stoabs.Writer {
	return t.store.getShelf(t.ctx, shelfName, t.writer, t.writer)
}

func (t tx) GetShelfReader(shelfName string) stoabs.Reader {
	return t.store.getShelf(t.ctx, shelfName, t.reader, nil)
}

func (t tx) Store() stoabs.KVStore {
	return t.store
}

func (t tx) Unwrap() interface{} {
	if t.writer != nil {
		return t.writer
	}
	return t.reader
}

type shelf struct {
	name      string
	reader    queryable
	writer    *sql.Tx
	store     *store
	tableName string
	ctx       context.Context
}

func (s shelf) Empty() (bool, error) {
	row := s.reader.QueryRowContext(s.ctx, fmt.Sprintf("SELECT COUNT(key) FROM %s", s.tableName))
	if errors.Is(row.Err(), sql.ErrNoRows) {
		return true, nil
	} else if row.Err() != nil {
		return false, stoabs.DatabaseError(row.Err())
	}
	var count int
	err := row.Scan(&count)
	if err != nil {
		return false, stoabs.DatabaseError(err)
	}
	return count == 0, nil
}

func (s shelf) Put(key stoabs.Key, value []byte) error {
	if err := s.store.checkOpen(); err != nil {
		return err
	}

	keyValue, err := s.getKeyValue(key)
	if err != nil {
		return err
	}

	updated, err := s.updateRecord(keyValue, value)
	if err != nil {
		return err
	}
	if updated {
		return nil
	}
	// Doesn't exist yet, upsert
	result, err := s.writer.ExecContext(s.ctx, fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2)", s.tableName), keyValue, value)
	if err != nil {
		return stoabs.DatabaseError(fmt.Errorf("error inserting record: %w", err))
	}
	rowsInserted, err := result.RowsAffected()
	if err != nil {
		return stoabs.DatabaseError(fmt.Errorf("error determining rows affected after insert: %w", err))
	}
	if rowsInserted != 1 {
		return fmt.Errorf("unexpected number of rows inserted: %d (expected 1)", rowsInserted)
	}
	return nil
}

func (s shelf) updateRecord(keyValue interface{}, value []byte) (bool, error) {
	result, err := s.writer.ExecContext(s.ctx, fmt.Sprintf("UPDATE %s SET value = $1 WHERE key = $2", s.tableName), value, keyValue)
	if err != nil {
		return false, stoabs.DatabaseError(fmt.Errorf("error updating record: %w", err))
	}
	rowsUpdated, err := result.RowsAffected()
	if err != nil {
		return false, stoabs.DatabaseError(fmt.Errorf("error determining rows affected after uodate: %w", err))
	}
	if rowsUpdated != 0 {
		// OK, updated
		return true, nil
	}
	return false, nil
}

func (s shelf) Delete(key stoabs.Key) error {
	keyValue, err := s.getKeyValue(key)
	if err != nil {
		return err
	}
	_, err = s.writer.ExecContext(s.ctx, fmt.Sprintf("DELETE FROM %s WHERE key = $1", s.tableName), keyValue)
	if err != nil {
		return stoabs.DatabaseError(err)
	}
	return err
}

func (s shelf) getKeyValue(key stoabs.Key) (interface{}, error) {
	var keyValue interface{}
	switch k := key.(type) {
	case stoabs.Uint32Key:
		keyValue = k.Bytes()
	case stoabs.BytesKey:
		keyValue = k.Bytes()
	case stoabs.HashKey:
		keyValue = k.Bytes()
	default:
		return nil, fmt.Errorf("unsupported key type: %T", key)
	}
	return keyValue, nil
}

func (s shelf) Get(key stoabs.Key) ([]byte, error) {
	keyValue, err := s.getKeyValue(key)
	if err != nil {
		return nil, err
	}
	var value []byte
	err = s.reader.QueryRowContext(s.ctx, fmt.Sprintf("SELECT value FROM %s WHERE key = $1", s.tableName), keyValue).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, stoabs.ErrKeyNotFound
	} else if err != nil {
		return nil, stoabs.DatabaseError(err)
	}
	return []byte(value), nil
}

func (s shelf) Iterate(callback stoabs.CallerFn, keyType stoabs.Key) error {
	rows, err := s.reader.QueryContext(s.ctx, fmt.Sprintf("SELECT key, value FROM %s", s.tableName))
	if err != nil {
		return stoabs.DatabaseError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var key, value []byte
		err = rows.Scan(&key, &value)
		if err != nil {
			return stoabs.DatabaseError(err)
		}
		typedKey, err := keyType.FromBytes(key)
		if err != nil {
			return fmt.Errorf("error converting key to type %T (%w): %v", keyType, err, key)
		}
		if err := callback(typedKey, value); err != nil {
			return err
		}
	}
	return nil
}

func (s shelf) Range(from stoabs.Key, to stoabs.Key, callback stoabs.CallerFn, stopAtNil bool) error {
	// Iterate from..to (start inclusive, end exclusive)
	rows, err := s.reader.QueryContext(s.ctx, fmt.Sprintf("SELECT key, value FROM %s WHERE key >= $1 AND key < $2", s.tableName), from.Bytes(), to.Bytes())
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return stoabs.DatabaseError(err)
	}
	defer rows.Close()

	currKey := from
	for rows.Next() {
		// Potentially long-running operation, check context for cancellation
		if s.ctx.Err() != nil {
			return stoabs.DatabaseError(s.ctx.Err())
		}
		var key, value []byte
		err = rows.Scan(&key, &value)
		if stopAtNil && !bytes.Equal(currKey.Bytes(), key) {
			// We've reached a missing key, and we should stop
			break
		}
		if err != nil {
			return stoabs.DatabaseError(err)
		}
		if err := callback(stoabs.BytesKey(key), value); err != nil {
			return err
		}
		currKey = currKey.Next()
	}
	return nil
}

func (s shelf) Stats() stoabs.ShelfStats {
	var numEntries uint
	err := s.reader.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(key) FROM %s", s.tableName)).Scan(&numEntries)
	if err != nil {
		logrus.WithError(err).Errorf("error getting shelf stats for table: %s", s.tableName)
	}
	return stoabs.ShelfStats{
		NumEntries: numEntries,
		ShelfSize:  0,
	}
}
