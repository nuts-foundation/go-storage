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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUint32Key_Next(t *testing.T) {
	key := Uint32Key(1)

	assert.Equal(t, "2", key.Next().String())
}

func TestBytesKey_Next(t *testing.T) {
	key := BytesKey([]byte{0x09})

	assert.Equal(t, "0a", key.Next().String())
}

func TestSha256Key_Next(t *testing.T) {
	hex1 := "a40d35e4d56273e633ef7bbf8f1e97aabe74ccc3510bd9a9a07493eaf5f815d5"
	hex2 := "a40d35e4d56273e633ef7bbf8f1e97aabe74ccc3510bd9a9a07493eaf5f815d6"
	bytes, _ := hex.DecodeString(hex1)
	key := NewSha256Key(*(*[32]byte)(bytes))

	assert.Equal(t, hex2, key.Next().String())
}
