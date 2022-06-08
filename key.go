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
}

// Uint32Key is a type helper for a uint32 as Key
type Uint32Key uint32

func (u Uint32Key) String() string {
	return fmt.Sprintf("%d", u)
}

func (u Uint32Key) Bytes() []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes[:], uint32(u))
	return bytes
}

func (u Uint32Key) Next() Key {
	return u + 1
}

// Sha256Key is a type helper for a 256 bits hash as Key
type Sha256Key [32]byte

// NewSha256Key creates a new Sha256Key from bytes
func NewSha256Key(bytes [32]byte) Key {
	return Sha256Key(bytes)
}

// String returns the hex encoding of the 256 bits hash
func (s Sha256Key) String() string {
	return hex.EncodeToString(s[:])
}

func (s Sha256Key) Bytes() []byte {
	return s[:]
}

func (s Sha256Key) Next() Key {
	next := &big.Int{}
	add := big.NewInt(1)
	next.SetBytes(s[:])
	next.Add(next, add)
	bytes := next.Bytes()
	return Sha256Key(*(*[32]byte)(bytes[:32]))
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
