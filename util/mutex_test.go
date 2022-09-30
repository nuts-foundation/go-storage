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

package util

import (
	"context"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_ContextRWLocker(t *testing.T) {
	t.Run("lock, unlock, then lock again", func(t *testing.T) {
		l := ContextRWLocker{}
		err := l.LockContext(context.Background())
		l.Unlock()
		assert.NoError(t, err)

		err = l.LockContext(context.Background())
		assert.NoError(t, err)
	})
	t.Run("rlock, unlock, then lock again", func(t *testing.T) {
		l := ContextRWLocker{}
		err := l.RLockContext(context.Background())
		l.RUnlock()
		assert.NoError(t, err)

		err = l.RLockContext(context.Background())
		assert.NoError(t, err)
	})
	t.Run("wlock, rlock", func(t *testing.T) {
		l := &ContextRWLocker{}

		err := l.LockContext(context.Background())
		assert.NoError(t, err)
		l.Unlock()

		err = l.RLockContext(context.Background())
		assert.NoError(t, err)
		l.RUnlock()

		err = l.LockContext(context.Background())
		assert.NoError(t, err)
		l.Unlock()
	})
	t.Run("rlock, rlock, unlock", func(t *testing.T) {
		l := &ContextRWLocker{}

		err := l.RLockContext(context.Background())
		assert.NoError(t, err)

		err = l.RLockContext(context.Background())
		assert.NoError(t, err)

		l.RUnlock()
		l.RUnlock()

		err = l.LockContext(context.Background())
		assert.NoError(t, err)
		l.Unlock()
	})
	t.Run("context cancelled", func(t *testing.T) {
		l := ContextRWLocker{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := l.LockContext(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		assert.ErrorIs(t, err, stoabs.ErrDatabase{})
	})
	t.Run("context timeout", func(t *testing.T) {
		l := ContextRWLocker{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()

		err := l.LockContext(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.ErrorIs(t, err, stoabs.ErrDatabase{})
	})
}
