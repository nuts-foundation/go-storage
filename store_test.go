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

package stoabs

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.NotEmpty(t, cfg.LockAcquireTimeout)
	assert.NotNil(t, cfg.Log)
}

func TestAcquireLockTimeout(t *testing.T) {
	cfg := DefaultConfig()
	WithLockAcquireTimeout(time.Hour)(&cfg)
	assert.Equal(t, time.Hour, cfg.LockAcquireTimeout)
}

func TestWriteLockOption(t *testing.T) {
	assert.True(t, WriteLockOption{}.Enabled([]TxOption{WithWriteLock()}))
	assert.False(t, WriteLockOption{}.Enabled([]TxOption{}))
}

func TestDatabaseError(t *testing.T) {
	t.Run("wraps db errors", func(t *testing.T) {
		assert.ErrorAs(t, ErrStoreIsClosed, new(ErrDatabase), "ErrStoreIsClosed should be a ErrDatabase")
		assert.ErrorAs(t, ErrCommitFailed, new(ErrDatabase), "ErrCommitFailed should be a ErrDatabase")
	})
	t.Run("does not wrap non-db errors", func(t *testing.T) {
		assert.False(t, errors.As(ErrKeyNotFound, new(ErrDatabase)), "ErrKeyNotFound is not a ErrDatabase")
	})
	t.Run("does not double wrap", func(t *testing.T) {
		firstError := DatabaseError(errors.New("this is wrapped"))
		secondError := DatabaseError(fmt.Errorf("this is not wrapped: %w", firstError))
		target := new(ErrDatabase)

		assert.ErrorAs(t, firstError, new(ErrDatabase))
		assert.ErrorAs(t, secondError, target)
		assert.ErrorIs(t, target, firstError)
	})
}

func TestNewErrorWriter(t *testing.T) {
	t.Run("it wraps an DatabaseError", func(t *testing.T) {
		writer := NewErrorWriter(errors.New("test"))

		_, err := writer.Get(BytesKey{})

		assert.ErrorIs(t, ErrDatabase{}, err)
	})
}

func TestNilReader_Get(t *testing.T) {
	t.Run("returns error", func(t *testing.T) {
		data, err := NilReader{}.Get(BytesKey{})
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, data)
	})
}
