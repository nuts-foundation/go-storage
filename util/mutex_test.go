package util

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
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
}
