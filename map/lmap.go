package _map

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/liuzhaodong89/lockfree-collection/common"
	"github.com/shopspring/decimal"
	"math"
	"sync"
	"sync/atomic"
	"time"
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
const DEFAULT_CAPACITY int32 = 256
const LOAD_FACTOR float32 = 0.75
const MULTIPLE_FACTOR = 4
const DEFAULT_BUCKET_CAPACITY = 10

func New() (m *lmap) {
	lm := lmap{
		resizeThreshold: int32(decimal.NewFromInt32(DEFAULT_CAPACITY).Mul(decimal.NewFromInt(DEFAULT_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(LOAD_FACTOR)).IntPart()),
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
	//hashkey := common.GetCityHashUseString(m.seed1, m.seed2, key.(string), 1)
	bucket := m.getLBucket(hashkey, m.buckets)

	return bucket.Get(key, hashkey)
}

func (m *lmap) Set(key interface{}, val any) bool {
	if m.count > m.resizeThreshold {
		m.resize()
	}

	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.set2bucket(key, val, m.buckets)
}

func (m *lmap) set2bucket(key interface{}, val any, buckets []*lbucket) bool {
	if key == nil {
		return false
	}

	hashkey := common.GetHash(key, m.seed1, m.seed2)
	//hashkey := common.GetCityHashUseString(m.seed1, m.seed2, key.(string), 1)

	bucket := m.getLBucketWithoutLock(hashkey, buckets)
	insert, succ := bucket.Set(key, hashkey, val)
	if insert == 1 && succ {
		atomic.AddInt32(&m.count, 1)
	}
	return succ
}

func (m *lmap) Del(key interface{}) (success bool) {
	if key == nil {
		return false
	}
	hashkey := common.GetHash(key, m.seed1, m.seed2)
	//hashkey := common.GetCityHashUseString(m.seed1, m.seed2, key.(string), 1)

	m.lock.RLock()
	defer m.lock.RUnlock()

	bucket := m.getLBucketWithoutLock(hashkey, m.buckets)
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
	startTime := time.Now()
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.count <= m.resizeThreshold {
		return
	}

	oldCap := m.capability

	if oldCap > 0 {
		if oldCap >= MAX_CAPACITY {
			m.resizeThreshold = MAX_CAPACITY
		} else {
			newCap := oldCap
			if oldCap >= MAX_CAPACITY/MULTIPLE_FACTOR {
				newCap = MAX_CAPACITY
			} else {
				newCap = oldCap * MULTIPLE_FACTOR
			}

			newThres := int32(decimal.NewFromInt32(newCap).Mul(decimal.NewFromInt(DEFAULT_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(LOAD_FACTOR)).IntPart())
			m.resizeThreshold = newThres
			m.capability = newCap
			//stageTime1 := time.Now()
			//fmt.Printf("stage time for calculate new params:%s \n", stageTime1.Sub(startTime))

			//newBuckets := m.createBuckets(newCap)

			var fi int32 = 0
			newBuckets := make([]*lbucket, 0)
			for ; fi < MULTIPLE_FACTOR; fi++ {
				newBuckets = append(newBuckets, m.buckets...)
			}

			//stageTime2 := time.Now()
			//fmt.Printf("stage time for create new buckets slice:%s \n", stageTime2.Sub(stageTime1))
			m.bucketsCountBit = uint64(math.Log2(float64(len(newBuckets))))
			//endTime1 := time.Now()
			//fmt.Printf("time for expand buckets: %s \n", endTime1.Sub(startTime))

			//rehash
			if newBuckets != nil {
				for i := 0; i < len(newBuckets); i++ {
					if i%MULTIPLE_FACTOR != 0 {
						//newBuckets[i].count = 0
						newBuckets[i].head = nil
					}
				}
				//endTime2 := time.Now()
				//fmt.Printf("time for copy buckets: %s \n", endTime2.Sub(endTime1))

				hashesSlice := make([]uint64, MULTIPLE_FACTOR-1)
				for j := 0; j < len(newBuckets); j = (j + 1) * MULTIPLE_FACTOR {
					hashesSlice = hashesSlice[0:0]
					for x := 1; x < MULTIPLE_FACTOR; x++ {
						hashesSlice = append(hashesSlice, uint64(j+x)<<(64-m.bucketsCountBit))
					}
					nodes := newBuckets[j].Split(hashesSlice)
					if nodes != nil {
						for y := 0; y < len(nodes); y++ {
							newBuckets[j+y].head = nodes[y]
						}
					}
				}
				//endTime3 := time.Now()
				//fmt.Printf("time for move nodes: %s \n", endTime3.Sub(endTime2))
			}
			m.buckets = newBuckets
		}
	}
	endTime := time.Now()
	fmt.Printf("time for resize:%s ============  and count is %v ,and threshold is %v \n", endTime.Sub(startTime), m.count, m.resizeThreshold)
}

func (m *lmap) createBuckets(bucketsSize int32) (buckets []*lbucket) {
	if bucketsSize > 0 {
		var i int32 = 0
		startTime := time.Now()
		buckets = make([]*lbucket, bucketsSize)
		endTime1 := time.Now()
		fmt.Printf("create buckets stage 1:%s \n", endTime1.Sub(startTime))
		for ; i < bucketsSize; i++ {
			buckets[i] = NewBucket()
		}
		endTime2 := time.Now()
		fmt.Printf("create buckets stage 2:%s \n", endTime2.Sub(endTime1))
		return buckets
	} else {
		return nil
	}
}

func (m *lmap) getLBucket(hashKey uint64, buckets []*lbucket) (b *lbucket) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.getLBucketWithoutLock(hashKey, buckets)
}

func (m *lmap) getLBucketWithoutLock(hashKey uint64, buckets []*lbucket) (b *lbucket) {
	return buckets[hashKey>>(64-m.bucketsCountBit)]
}
