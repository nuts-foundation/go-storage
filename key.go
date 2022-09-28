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
	"strconv"
)

// Key is an abstraction for a key in a key/value pair. The underlying implementation determines if the string or byte representation is used.
type Key interface {
	fmt.Stringer
	// FromString creates a new instance of this Key, parses it from the given string and returns it. It does not modify this key.
	FromString(string) (Key, error)
	// Bytes returns a byte representation of this key
	Bytes() []byte
	// FromBytes creates a new instance of this Key, parses it from the given byte slice and returns it. It does not modify this key.
	FromBytes([]byte) (Key, error)
	// Next returns the next logical key. The next for the number 1 would be 2. The next for the string "a" would be "b"
	Next() Key
	// Equals returns true when the given key is of same type and value.
	Equals(other Key) bool
}

// Uint32Key is a type helper for a uint32 as Key
type Uint32Key uint32

func (u Uint32Key) FromBytes(i []byte) (Key, error) {
	if len(i) != 4 {
		return nil, fmt.Errorf("given bytes (len=%d) can't be parsed as %T", len(i), u)
	}
	return Uint32Key(binary.BigEndian.Uint32(i)), nil
}

func (u Uint32Key) String() string {
	return fmt.Sprintf("%d", u)
}

func (u Uint32Key) FromString(i string) (Key, error) {
	result, err := strconv.ParseInt(i, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("given string can't be parsed as %T: %w", u, err)
	}
	if result < 0 {
		return nil, fmt.Errorf("given string can't be parsed as %T, because it yields a signed integer", u)
	}
	return Uint32Key(result), nil
}

func (u Uint32Key) Bytes() []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result[:], uint32(u))
	return result
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

func (s HashKey) String() string {
	return hex.EncodeToString(s[:])
}

func (s HashKey) FromString(i string) (Key, error) {
	asBytes, err := hex.DecodeString(i)
	if err != nil {
		return nil, fmt.Errorf("given string can't be parsed as %T: %w", s, err)
	}
	return s.FromBytes(asBytes)
}

func (s HashKey) FromBytes(i []byte) (Key, error) {
	if len(i) != len(s) {
		return nil, fmt.Errorf("given bytes (len=%d) can't be parsed as %T", len(i), s)
	}
	var result HashKey
	copy(result[:], i)
	return result, nil
}

func (s HashKey) Bytes() []byte {
	return s[:]
}

func (s HashKey) Next() Key {
	next := &big.Int{}
	add := big.NewInt(1)
	next.SetBytes(s[:])
	next.Add(next, add)
	result := next.Bytes()
	return HashKey(*(*[32]byte)(result[:32]))
}

func (s HashKey) Equals(other Key) bool {
	o, ok := other.(HashKey)
	return ok && o == s
}

// BytesKey is a type helper for a byte slice as Key
type BytesKey []byte

func (b BytesKey) String() string {
	return hex.EncodeToString(b)
}

func (b BytesKey) FromString(i string) (Key, error) {
	asBytes, err := hex.DecodeString(i)
	if err != nil {
		return nil, fmt.Errorf("given string can't be parsed as %T: %w", b, err)
	}
	return b.FromBytes(asBytes)
}

func (b BytesKey) Bytes() []byte {
	return b
}

func (b BytesKey) FromBytes(i []byte) (Key, error) {
	return BytesKey(i), nil
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
	return ok && bytes.Equal(b, o)
}
