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
)

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
		return stoabs.DatabaseError(ctx.Err())
	case err := <-closeError:
		// Function completed, maybe with error
		return err
	}
}
