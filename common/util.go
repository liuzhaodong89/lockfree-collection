package common

import (
	"fmt"
	"github.com/dchest/siphash"
	cityhash2 "github.com/zentures/cityhash"
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
		//return memhash(seed1, seed2+1, unsafe.Pointer(&v), 1)
		return uint64(v) * seed1
	case int8:
		//return memhash(seed1, seed2-1, unsafe.Pointer(&v), 1)
		return uint64(v) * seed2
	case uint16:
		//return memhash(seed1, seed2+1, unsafe.Pointer(&v), 2)
		return uint64(v) * seed1
	case int16:
		//return memhash(seed1, seed2-1, unsafe.Pointer(&v), 2)
		return uint64(v) * seed2
	case uint32:
		//return memhash(seed1, seed2+1, unsafe.Pointer(&v), 4)
		return uint64(v) * seed1
	case int32:
		//return memhash(seed1, seed2-1, unsafe.Pointer(&v), 4)
		return uint64(v) * seed2
	case uint64:
		return v * seed1
	case int64:
		//return memhash(seed1, seed2-1, unsafe.Pointer(&v), 8)
		return uint64(v) * seed2
	case uint:
		//return memhash(seed1, seed2+2, unsafe.Pointer(&v), intSize)
		return uint64(v) * seed1
	case int:
		//return memhash(seed1, seed2-2, unsafe.Pointer(&v), intSize)
		return uint64(v) * seed2
	case float32:
		uint32Value := math.Float32bits(v)
		return uint64(uint32Value) * seed1
		//return memhash(seed1, seed2+1, unsafe.Pointer(&uint32Value), 4)
		//return memhash(seed1, seed2+1, unsafe.Pointer(&v), 4)
	case float64:
		uint64Value := math.Float64bits(v)
		return uint64Value * seed2
		//return memhash(seed1, seed2+1, unsafe.Pointer(&uint64Value), 8)
		//return memhash(seed1, seed2+1, unsafe.Pointer(&v), 8)
	case bool:
		if v {
			return 1
		} else {
			return 0
		}
	case []byte:
		return siphash.Hash(seed1, seed2, v)
	case string:
		//hdr := (*reflect.StringHeader)(unsafe.Pointer(&v))
		//sh := reflect.SliceHeader{
		//	Data: hdr.Data,
		//	Len:  hdr.Len,
		//	Cap:  hdr.Len,
		//}
		//return siphash.Hash(seed1-1, seed2, *(*[]byte)(unsafe.Pointer(&sh)))
		return BKDRHash(v, seed1)
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

func BKDRHash(str string, seed uint64) uint64 {
	hash := uint64(0)
	strLen := len(str)
	for i := 0; i < strLen; i++ {
		hash = (hash * seed) + uint64(str[i])
	}
	return hash
}

func GetCityHashUseString(k0, k1 uint64, data string, length uint32) uint64 {
	return cityhash2.CityHash64WithSeeds([]byte(data), length, k0, k1)
}
