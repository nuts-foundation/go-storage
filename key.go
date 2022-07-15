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
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
)

// Key is an abstraction for a key in a key/value pair. The underlying implementation determines if the string or byte representation is used.
type Key interface {
	fmt.Stringer

	// Bytes returns a byte representation of this key
	Bytes() []byte
	// Next returns the next logical key. The next for the number 1 would be 2. The next for the string "a" would be "b"
	Next() Key
	// Equals returns true when the given key is of same type and value.
	Equals(other Key) bool
}

// Uint32Key is a type helper for a uint32 as Key
type Uint32Key uint32

func (u Uint32Key) String() string {
	return fmt.Sprintf("%d", u)
}

// Bytes outputs the byte representation of the uint32 in BigEndian
func (u Uint32Key) Bytes() []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes[:], uint32(u))
	return bytes
}

func (u Uint32Key) Next() Key {
	return u + 1
}

func (u Uint32Key) Equals(other Key) bool {
	o, ok := other.(Uint32Key)
	return ok && o == u
}

// HashKey is a type helper for a 256 bits hash as Key
type HashKey [32]byte

// NewHashKey creates a new HashKey from bytes
func NewHashKey(bytes [32]byte) Key {
	return HashKey(bytes)
}

// String returns the hex encoding of the 256 bits hash
func (s HashKey) String() string {
	return hex.EncodeToString(s[:])
}

func (s HashKey) Bytes() []byte {
	return s[:]
}

func (s HashKey) Next() Key {
	next := &big.Int{}
	add := big.NewInt(1)
	next.SetBytes(s[:])
	next.Add(next, add)
	bytes := next.Bytes()
	return HashKey(*(*[32]byte)(bytes[:32]))
}

func (s HashKey) Equals(other Key) bool {
	o, ok := other.(HashKey)
	return ok && o == s
}

// BytesKey is a type helper for a byte slice as Key
type BytesKey []byte

// String returns the byte slice as hex encoded
func (b BytesKey) String() string {
	return hex.EncodeToString(b)
}

func (b BytesKey) Bytes() []byte {
	return b
}

func (b BytesKey) Next() Key {
	next := &big.Int{}
	add := big.NewInt(1)
	next.SetBytes(b)
	next.Add(next, add)
	return BytesKey(next.Bytes())
}

func (b BytesKey) Equals(other Key) bool {
	o, ok := other.(BytesKey)
	return ok && bytes.Compare(b, o) == 0
}
