package common

import (
	"fmt"
	"github.com/dchest/siphash"
	"math"
	"reflect"
	"unsafe"
)

const (
	// intSize is the size in bytes of an int or uint value
	intSize = (32 << (^uint(0) >> 63)) >> 3
)

func GetHash(key interface{}, seed1 uint64, seed2 uint64) (hash uint64) {
	switch v := key.(type) {
	case uint8:
		return memhash(seed1, seed2+1, unsafe.Pointer(&v), 1)
	case int8:
		return memhash(seed1, seed2-1, unsafe.Pointer(&v), 1)
	case uint16:
		return memhash(seed1, seed2+1, unsafe.Pointer(&v), 2)
	case int16:
		return memhash(seed1, seed2-1, unsafe.Pointer(&v), 2)
	case uint32:
		return memhash(seed1, seed2+1, unsafe.Pointer(&v), 4)
	case int32:
		return memhash(seed1, seed2-1, unsafe.Pointer(&v), 4)
	case uint64:
		return v
	case int64:
		return memhash(seed1, seed2-1, unsafe.Pointer(&v), 8)
	case uint:
		return memhash(seed1, seed2+2, unsafe.Pointer(&v), intSize)
	case int:
		return memhash(seed1, seed2-2, unsafe.Pointer(&v), intSize)
	case float32:
		uint32Value := math.Float32bits(v)
		return memhash(seed1, seed2+1, unsafe.Pointer(&uint32Value), 4)
		//return memhash(seed1, seed2+1, unsafe.Pointer(&v), 4)
	case float64:
		uint64Value := math.Float64bits(v)
		return memhash(seed1, seed2+1, unsafe.Pointer(&uint64Value), 8)
		//return memhash(seed1, seed2+1, unsafe.Pointer(&v), 8)
	case bool:
		return memhash(seed1, seed2+1, unsafe.Pointer(&v), 1)
	case []byte:
		return siphash.Hash(seed1, seed2, v)
	case string:
		hdr := (*reflect.StringHeader)(unsafe.Pointer(&v))
		sh := reflect.SliceHeader{
			Data: hdr.Data,
			Len:  hdr.Len,
			Cap:  hdr.Len,
		}
		return siphash.Hash(seed1-1, seed2, *(*[]byte)(unsafe.Pointer(&sh)))
	default:
		panic(fmt.Errorf("unsupported key type %T", v))
	}
}

func memhash(k0, k1 uint64, addr unsafe.Pointer, size int) uint64 {
	sh := reflect.SliceHeader{
		Data: uintptr(addr),
		Len:  size,
		Cap:  size,
	}
	return siphash.Hash(k0, k1, *(*[]byte)(unsafe.Pointer(&sh)))
}
