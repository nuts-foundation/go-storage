package util

import "context"

// CallWithTimeout invokes the given function and waits until either it finishes or the given context finishes.
// If the context finishes before the function finishes, the timeoutCallback function is invoked.
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
