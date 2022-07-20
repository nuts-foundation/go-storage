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

func TestUint32Key_String(t *testing.T) {
	key := Uint32Key(1)

	actual, err := key.FromString(key.String())
	assert.NoError(t, err)
	assert.Equal(t, key, actual)
}

func TestUint32Key_Bytes(t *testing.T) {
	key := Uint32Key(1)
	expected := []byte{0, 0, 0, 1}

	assert.Equal(t, expected, key.Bytes())
}

func TestUint32Key_FromBytes(t *testing.T) {
	key := Uint32Key(1)
	keyBytes := key.Bytes()

	t.Run("ok", func(t *testing.T) {
		actual, err := key.FromBytes(keyBytes)
		assert.NoError(t, err)
		assert.Equal(t, key, actual)
		assert.True(t, key.Equals(actual))
	})
	t.Run("invalid length", func(t *testing.T) {
		actual, err := key.FromBytes([]byte{1})
		assert.EqualError(t, err, "given bytes (len=1) can't be parsed as stoabs.Uint32Key")
		assert.Nil(t, actual)
	})
}

func TestBytesKey_Next(t *testing.T) {
	key := BytesKey([]byte{0x09})

	assert.Equal(t, "0a", key.Next().String())
}

func TestBytesKey_String(t *testing.T) {
	key := BytesKey([]byte{0x09})

	actual, err := key.FromString(key.String())
	assert.NoError(t, err)
	assert.Equal(t, key, actual)
}

func TestBytesKey_Bytes(t *testing.T) {
	bytes := []byte{0x09}
	key := BytesKey(bytes)

	assert.Equal(t, bytes, key.Bytes())
}

func TestBytesKey_Equal(t *testing.T) {
	key := BytesKey([]byte{0x09})

	assert.True(t, key.Equals(BytesKey([]byte{0x09})))
	assert.False(t, key.Equals(key.Next()))
	assert.False(t, key.Equals(BytesKey("h")))
}

func TestBytesKey_FromBytes(t *testing.T) {
	input := []byte{0x09}
	key := BytesKey(input)

	actual, err := key.FromBytes(input)
	assert.NoError(t, err)
	assert.Equal(t, key, actual)
}

func TestHashKey_Next(t *testing.T) {
	hex1 := "a40d35e4d56273e633ef7bbf8f1e97aabe74ccc3510bd9a9a07493eaf5f815d5"
	hex2 := "a40d35e4d56273e633ef7bbf8f1e97aabe74ccc3510bd9a9a07493eaf5f815d6"
	bytes, _ := hex.DecodeString(hex1)
	key := NewHashKey(*(*[32]byte)(bytes))

	assert.Equal(t, hex2, key.Next().String())
}

func TestHashKey_String(t *testing.T) {
	bytes, _ := hex.DecodeString("a40d35e4d56273e633ef7bbf8f1e97aabe74ccc3510bd9a9a07493eaf5f815d5")
	key := NewHashKey(*(*[32]byte)(bytes))

	actual, err := key.FromString(key.String())
	assert.NoError(t, err)
	assert.Equal(t, key, actual)
}

func TestHashKey_Bytes(t *testing.T) {
	hex1 := "a40d35e4d56273e633ef7bbf8f1e97aabe74ccc3510bd9a9a07493eaf5f815d5"
	bytes, _ := hex.DecodeString(hex1)
	key := NewHashKey(*(*[32]byte)(bytes))

	bytesKey := BytesKey(key.Bytes())

	assert.Equal(t, hex1, bytesKey.String())
}

func TestHashKey_Equals(t *testing.T) {
	bytes, _ := hex.DecodeString("a40d35e4d56273e633ef7bbf8f1e97aabe74ccc3510bd9a9a07493eaf5f815d5")
	key := NewHashKey(*(*[32]byte)(bytes))

	assert.True(t, key.Equals(key))
	assert.False(t, key.Equals(key.Next()))
	assert.False(t, key.Equals(BytesKey("h")))
}

func TestHashKey_FromBytes(t *testing.T) {
	bytes, _ := hex.DecodeString("a40d35e4d56273e633ef7bbf8f1e97aabe74ccc3510bd9a9a07493eaf5f815d5")
	key := NewHashKey(*(*[32]byte)(bytes))

	t.Run("ok", func(t *testing.T) {
		actual, err := key.FromBytes(bytes)
		assert.NoError(t, err)
		assert.Equal(t, key, actual)
		assert.True(t, key.Equals(actual))
	})
	t.Run("invalid length", func(t *testing.T) {
		actual, err := key.FromBytes([]byte{1})
		assert.EqualError(t, err, "given bytes (len=1) can't be parsed as stoabs.HashKey")
		assert.Nil(t, actual)
	})
}
