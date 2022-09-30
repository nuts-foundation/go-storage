package util

import (
	"context"
	"errors"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_CallWithTimeout(t *testing.T) {
	testTimeout := 5 * time.Millisecond
	t.Run("ok", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		var didSomething, timeoutFnCalled bool

		err := CallWithTimeout(ctx, func() error {
			didSomething = true
			return nil
		}, func() { timeoutFnCalled = true })

		assert.NoError(t, err)
		assert.True(t, didSomething)
		assert.False(t, timeoutFnCalled)
	})

	t.Run("err", func(t *testing.T) {
		t.Run("context.Canceled is ErrDatabase", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			cancel()
			var timeoutFnCalled bool

			err := CallWithTimeout(ctx, func() error {
				return nil
			}, func() { timeoutFnCalled = true })

			assert.ErrorIs(t, err, context.Canceled)
			assert.ErrorIs(t, err, stoabs.ErrDatabase{})
			assert.True(t, timeoutFnCalled)
		})

		t.Run("context.DeadlineExceeded is ErrDatabase", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
			defer cancel()
			var timeoutFnCalled bool

			err := CallWithTimeout(ctx, func() error {
				time.Sleep(testTimeout)
				return nil
			}, func() { timeoutFnCalled = true })

			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.ErrorIs(t, err, stoabs.ErrDatabase{})
			assert.True(t, timeoutFnCalled)
		})

		t.Run("user defined is not an ErrDatabase", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()
			var timeoutFnCalled bool

			err := CallWithTimeout(ctx, func() error {
				return errors.New("custom error")
			}, func() { timeoutFnCalled = true })

			assert.EqualError(t, err, "custom error")
			assert.NotErrorIs(t, err, stoabs.ErrDatabase{})
			assert.False(t, timeoutFnCalled)
		})
	})
}
