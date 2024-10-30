package _map

import (
	"crypto/rand"
	"encoding/binary"
	"github.com/liuzhaodong89/lockfree-collection/common"
	"github.com/shopspring/decimal"
	"math"
	"sync"
	"sync/atomic"
)

type lmap struct {
	count int32
	//bucketsCapacity int32
	buckets         []*lbucket
	bucketsCountBit uint64

	resizeThreshold int32
	lock            sync.RWMutex
	seed1, seed2    uint64
}

const MAX_BUCKETS_CAPACITY int32 = math.MaxInt32
const DEFAULT_BUCKETS_CAPACITY int32 = 128
const EXPAND_THRES_FACTOR float32 = 0.4
const EXPAND_FACTOR = 4
const NODES_PER_BUCKET_CAPACITY = 10

func New() (m *lmap) {
	lm := lmap{
		resizeThreshold: int32(decimal.NewFromInt32(DEFAULT_BUCKETS_CAPACITY).Mul(decimal.NewFromInt(NODES_PER_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(EXPAND_THRES_FACTOR)).IntPart()),
		//bucketsCapacity: DEFAULT_BUCKETS_CAPACITY,
		count:   0,
		buckets: make([]*lbucket, DEFAULT_BUCKETS_CAPACITY),
	}

	lm.bucketsCountBit = uint64(math.Log2(float64(len(lm.buckets))))

	binary.Read(rand.Reader, binary.BigEndian, &lm.seed1)
	binary.Read(rand.Reader, binary.BigEndian, &lm.seed2)

	lm.buckets = lm.createBuckets(DEFAULT_BUCKETS_CAPACITY)

	return &lm
}

func (m *lmap) Get(key interface{}) (val any, exist bool) {
	if key == nil {
		return nil, false
	}

	m.lock.RLock()
	defer m.lock.RUnlock()

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
	//startTime := time.Now()
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.count <= m.resizeThreshold {
		return
	}
	//fmt.Printf("before resize resizethreshold is %s, and count is %s, and buckets count is %s, and bucket capacity is %s \n", m.resizeThreshold, m.count, len(m.buckets), m.bucketsCapacity)

	currentBucketsCapacity := int32(len(m.buckets))
	newBucketsCapacity := currentBucketsCapacity

	if currentBucketsCapacity < MAX_BUCKETS_CAPACITY {
		if currentBucketsCapacity*EXPAND_FACTOR >= MAX_BUCKETS_CAPACITY {
			newBucketsCapacity = MAX_BUCKETS_CAPACITY
		} else {
			newBucketsCapacity = currentBucketsCapacity * EXPAND_FACTOR
		}
	}

	if newBucketsCapacity == currentBucketsCapacity {
		return
	}

	//m.bucketsCapacity = newBucketsCapacity
	m.resizeThreshold = int32(decimal.NewFromInt32(newBucketsCapacity).Mul(decimal.NewFromInt32(NODES_PER_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(EXPAND_THRES_FACTOR)).IntPart())

	//for i := 0; i < int(math.Log2(float64(EXPAND_FACTOR))); i++ {
	//	m.buckets = append(m.buckets, m.buckets...)
	//}
	appendingBuckets := m.createBuckets(int32((EXPAND_FACTOR - 1) * len(m.buckets)))
	m.buckets = append(m.buckets, appendingBuckets...)

	atomic.AddUint64(&m.bucketsCountBit, uint64(math.Log2(float64(EXPAND_FACTOR))))
	//fmt.Printf("buckets size: %s and designed buckets size: %s and bucketsCountBit: %s \n", len(nBuckets), m.bucketsCapacity, m.bucketsCountBit)

	//将原先第i个bucket，移动至第EXPAND_FACTOR*i个位置
	for i := len(m.buckets)/EXPAND_FACTOR - 1; i >= 0; i-- {
		//m.buckets[i*EXPAND_FACTOR].head = nil
		//m.buckets[i*EXPAND_FACTOR].count = 0
		m.buckets[i*EXPAND_FACTOR] = m.buckets[i]

		//for j := 1; j < EXPAND_FACTOR; j++ {
		//	m.buckets[i*EXPAND_FACTOR+j] = NewBucket()
		//	//m.buckets[i*EXPAND_FACTOR+j].count = 0
		//}
	}

	//将第EXPAND_FACTOR*i个bucket内的nodes重新切分，分配到EXPAND_FACTOR*(i-1)+1到EXPAND_FACTOR*i-1个bucket内
	for i := 0; i < len(m.buckets); i = i + 4 {
		originHead := m.buckets[i].head
		m.buckets[i].head = nil
		for current := originHead; current != nil; {
			bucketIndex := current.hashVal >> (64 - m.bucketsCountBit)

			newNode := current
			current = current.GetNext()

			newNode.nextPointer = nil
			if m.buckets[bucketIndex].head != nil {
				newNode.nextPointer = m.buckets[bucketIndex].head
				m.buckets[bucketIndex].head = newNode

				//atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&newNode.nextPointer)), nil, unsafe.Pointer(&m.buckets[bucketIndex].head))
				//atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&m.buckets[bucketIndex].head)), unsafe.Pointer(m.buckets[bucketIndex].head), unsafe.Pointer(&newNode))
			} else {
				m.buckets[bucketIndex].head = newNode
			}
		}
	}
	//endTime := time.Now()
	//fmt.Printf("after resize resizethreshold is %s, and count is %s, and buckets count is %s, and bucket capacity is %s \n", m.resizeThreshold, m.count, len(m.buckets), m.bucketsCapacity)
	//fmt.Printf("time for resize:%s ============ \n", endTime.Sub(startTime))
}

func (m *lmap) createBuckets(bucketsSize int32) (buckets []*lbucket) {
	if bucketsSize > 0 {
		var i int32 = 0
		//startTime := time.Now()
		buckets = make([]*lbucket, bucketsSize)
		//endTime1 := time.Now()
		//fmt.Printf("create buckets stage 1:%s \n", endTime1.Sub(startTime))
		for ; i < bucketsSize; i++ {
			buckets[i] = NewBucket()
		}
		//endTime2 := time.Now()
		//fmt.Printf("create buckets stage 2:%s \n", endTime2.Sub(endTime1))
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
