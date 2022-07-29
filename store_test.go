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
