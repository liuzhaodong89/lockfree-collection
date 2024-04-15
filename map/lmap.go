package _map

import (
	"crypto/rand"
	"encoding/binary"
	"github.com/shopspring/decimal"
	"liuzhaodong.com/lockfree-collection/common"
	"math"
	"sync"
	"sync/atomic"
)

type lmap struct {
	count           int32
	capability      int32
	buckets         []*lbucket
	bucketsCountBit uint64

	resizeThreshold int32
	lock            sync.RWMutex
	seed1, seed2    uint64
}

const MAX_CAPACITY int32 = math.MaxInt32
const DEFAULT_CAPACITY int32 = 16
const LOAD_FACTOR float32 = 0.75

func New() (m *lmap) {
	lm := lmap{
		resizeThreshold: int32(decimal.NewFromInt32(DEFAULT_CAPACITY).Mul(decimal.NewFromFloat32(LOAD_FACTOR)).IntPart()),
		capability:      DEFAULT_CAPACITY,
		count:           0,
		buckets:         make([]*lbucket, DEFAULT_CAPACITY),
	}

	lm.bucketsCountBit = uint64(math.Log2(float64(len(lm.buckets))))

	binary.Read(rand.Reader, binary.BigEndian, &lm.seed1)
	binary.Read(rand.Reader, binary.BigEndian, &lm.seed2)

	lm.buckets = lm.createBuckets(DEFAULT_CAPACITY)

	return &lm
}

func (m *lmap) Get(key interface{}) (val any, exist bool) {
	if key == nil {
		return nil, false
	}

	hashkey := common.GetHash(key, m.seed1, m.seed2)
	bucket := m.getLBucket(hashkey, m.buckets)

	return bucket.Get(key, hashkey)
}

func (m *lmap) Set(key interface{}, val any) bool {
	if m.count > m.resizeThreshold {
		m.resize()
	}
	return m.set2buckest(key, val, m.buckets)
}

func (m *lmap) set2buckest(key interface{}, val any, buckets []*lbucket) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if key == nil {
		return false
	}

	hashkey := common.GetHash(key, m.seed1, m.seed2)
	bucket := m.getLBucket(hashkey, buckets)
	if nil == bucket {
		bucket = NewBucket()
		buckets = append(buckets, bucket)
	}
	atomic.AddInt32(&m.count, 1)

	return bucket.Set(key, hashkey, val)
}

func (m *lmap) Del(key interface{}) (success bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if key == nil {
		return false
	}
	hashkey := common.GetHash(key, m.seed1, m.seed2)
	bucket := m.getLBucket(hashkey, m.buckets)

	suc := bucket.delete(key, hashkey)
	if suc {
		atomic.AddInt32(&m.count, -1)
	}
	return suc
}

func (m *lmap) Size() (size int32) {
	return m.count
}

func (m *lmap) resize() {
	m.lock.Lock()
	defer m.lock.Unlock()

	oldCap := m.capability
	oldBuckets := m.buckets

	if oldCap > 0 {
		if oldCap >= MAX_CAPACITY {
			m.resizeThreshold = MAX_CAPACITY
		} else {
			newCap := oldCap
			if oldCap >= MAX_CAPACITY/2 {
				newCap = MAX_CAPACITY
			} else {
				newCap = oldCap * 2
			}

			newThres := int32(decimal.NewFromInt32(newCap).Mul(decimal.NewFromFloat32(LOAD_FACTOR)).IntPart())
			m.resizeThreshold = newThres

			newBuckets := m.createBuckets(newCap)

			//rehash
			if oldBuckets != nil {
				for i := 0; i < len(oldBuckets); i++ {
					b := oldBuckets[i]
					if b != nil {
						node := b.head
						for {
							if node == nil {
								break
							}
							m.set2buckest(node.GetKeyAtomically(), *(*interface{})(node.GetValueAtomically()), newBuckets)
							node = node.nextPointer
						}
					}
				}
			}
			m.buckets = newBuckets
		}
	}
}

func (m *lmap) createBuckets(bucketsSize int32) (buckets []*lbucket) {
	if bucketsSize > 0 {
		var i int32 = 0
		buckets = make([]*lbucket, bucketsSize)
		for ; i < bucketsSize; i++ {
			buckets[i] = NewBucket()
		}
		return buckets
	} else {
		return nil
	}
}

func (m *lmap) getLBucket(hashKey uint64, buckets []*lbucket) (b *lbucket) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return buckets[hashKey>>(64-m.bucketsCountBit)]
}
