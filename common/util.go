package common

import (
	"fmt"
	"github.com/dchest/siphash"
	"math"
	"unsafe"
)

const (
	// intSize is the size in bytes of an int or uint value
	intSize = (32 << (^uint(0) >> 63)) >> 3
)

type Hashable interface {
	Hash64() uint64
}

func GetHash(key interface{}, seed1 uint64, seed2 uint64) (hash uint64) {
	switch v := key.(type) {
	case string:
		return BKDRHash(v, seed1)
	case uint8:
		return uint64(v) * seed1
	case int8:
		return uint64(v) * seed1
	case uint16:
		return uint64(v) * seed1
	case int16:
		return uint64(v) * seed1
	case uint32:
		return uint64(v) * seed1
	case int32:
		return uint64(v) * seed1
	case uint64:
		return v * seed1
	case int64:
		return uint64(v) * seed1
	case uint:
		return uint64(v) * seed1
	case int:
		return uint64(v) * seed1
	case float32:
		uint32Value := math.Float32bits(v)
		return uint64(uint32Value) * seed1
	case float64:
		uint64Value := math.Float64bits(v)
		return uint64Value * seed1
	case bool:
		if v {
			return 1
		} else {
			return 0
		}
	case []byte:
		return siphash.Hash(seed1, seed2, v)
	case *string:
		strPtr := unsafe.Pointer(v)
		return uint64(uintptr(strPtr)) * seed1
	default:
		if h, ok := v.(Hashable); ok {
			return h.Hash64()
		}
		panic(fmt.Errorf("unsupported key type %T", v))
	}
}

func BKDRHash(str string, seed uint64) uint64 {
	hash := uint64(0)
	strLen := len(str)
	for i := 0; i < strLen; i++ {
		hash = (hash * seed) + uint64(str[i])
	}
	return hash
}
