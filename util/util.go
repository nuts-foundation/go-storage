package util

import "context"

func CallWithTimeout(ctx context.Context, fn func() error, timeoutCallback func()) error {
	closeError := make(chan error)
	go func() {
		closeError <- fn()
	}()
	select {
	case <-ctx.Done():
		timeoutCallback()
		return ctx.Err()
	case err := <-closeError:
		// Function completed, maybe with error
		return err
	}
}
