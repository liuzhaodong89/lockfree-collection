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

type Lmap struct {
	count           int32
	buckets         []*lbucket
	bucketsCountBit uint64

	expandThreshold int32
	reduceThreshold int32
	lock            sync.RWMutex
	seed1, seed2    uint64
}

const MAX_BUCKETS_CAPACITY int32 = math.MaxInt32
const DEFAULT_BUCKETS_CAPACITY int32 = 128
const EXPAND_THRES_FACTOR float32 = 0.4
const EXPAND_FACTOR = 4
const NODES_PER_BUCKET_CAPACITY = 10
const REDUCE_THRES_FACTOR float32 = 0.25
const REDUCE_FACTOR = 4

func New() (m *Lmap) {
	lm := Lmap{
		expandThreshold: int32(decimal.NewFromInt32(DEFAULT_BUCKETS_CAPACITY).Mul(decimal.NewFromInt(NODES_PER_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(EXPAND_THRES_FACTOR)).IntPart()),
		//bucketsCapacity: DEFAULT_BUCKETS_CAPACITY,
		reduceThreshold: 0,
		count:           0,
		buckets:         make([]*lbucket, DEFAULT_BUCKETS_CAPACITY),
	}

	lm.bucketsCountBit = uint64(math.Log2(float64(len(lm.buckets))))

	binary.Read(rand.Reader, binary.BigEndian, &lm.seed1)
	binary.Read(rand.Reader, binary.BigEndian, &lm.seed2)

	lm.buckets = lm.createBuckets(DEFAULT_BUCKETS_CAPACITY)

	return &lm
}

func (m *Lmap) Get(key interface{}) (val any, exist bool) {
	if key == nil {
		return nil, false
	}

	m.lock.RLock()
	defer m.lock.RUnlock()

	hashkey := common.GetHash(key, m.seed1, m.seed2)
	bucket := m.getLBucket(hashkey, m.buckets)

	return bucket.Get(key, hashkey)
}

func (m *Lmap) Set(key interface{}, val any) bool {
	if m.count > m.expandThreshold {
		m.expand()
	}

	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.set2bucket(key, val, m.buckets)
}

func (m *Lmap) set2bucket(key interface{}, val any, buckets []*lbucket) bool {
	if key == nil {
		return false
	}

	hashkey := common.GetHash(key, m.seed1, m.seed2)
	//hashkey := common.GetCityHashUseString(m.seed1, m.seed2, key.(string), 1)

	bucket := m.getLBucketWithoutLock(hashkey, buckets)
	insert, succ := bucket.Set(key, hashkey, val)
	if insert == 1 {
		//retry two times
		if !succ {
			_, succ = bucket.Set(key, hashkey, val)
			if !succ {
				_, succ = bucket.Set(key, hashkey, val)
			}
		}

		if succ {
			atomic.AddInt32(&m.count, 1)
		}
	}

	return succ
}

func (m *Lmap) Del(key interface{}) (success bool) {
	if key == nil {
		return false
	}

	if m.count < m.reduceThreshold {
		m.reduce()
	}

	m.lock.RLock()
	defer m.lock.RUnlock()

	hashkey := common.GetHash(key, m.seed1, m.seed2)
	bucket := m.getLBucketWithoutLock(hashkey, m.buckets)
	suc := bucket.delete(key, hashkey)
	if suc {
		atomic.AddInt32(&m.count, -1)
	}
	return suc
}

func (m *Lmap) Size() (size int32) {
	return m.count
}

func (m *Lmap) expand() {
	//startTime := time.Now()
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.count <= m.expandThreshold {
		return
	}
	//fmt.Printf("before expand resizethreshold is %s, and count is %s, and buckets count is %s \n", m.expandThreshold, m.count, len(m.buckets))

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

	m.expandThreshold = int32(decimal.NewFromInt32(newBucketsCapacity).Mul(decimal.NewFromInt32(NODES_PER_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(EXPAND_THRES_FACTOR)).IntPart())
	m.reduceThreshold = int32(decimal.NewFromInt32(newBucketsCapacity).Mul(decimal.NewFromFloat32(REDUCE_THRES_FACTOR)).IntPart())
	atomic.AddUint64(&m.bucketsCountBit, uint64(math.Log2(float64(EXPAND_FACTOR))))

	appendingBuckets := m.createBuckets(int32((EXPAND_FACTOR - 1) * len(m.buckets)))
	m.buckets = append(m.buckets, appendingBuckets...)
	//fmt.Printf("buckets size: %s and designed buckets size: %s and bucketsCountBit: %s \n", len(nBuckets), m.bucketsCapacity, m.bucketsCountBit)

	//将原先第i个bucket，移动至第EXPAND_FACTOR*i个位置
	for i := len(m.buckets)/EXPAND_FACTOR - 1; i >= 0; i-- {
		m.buckets[i*EXPAND_FACTOR] = m.buckets[i]
	}

	//将第EXPAND_FACTOR*i个bucket内的nodes重新切分，分配到EXPAND_FACTOR*(i-1)+1到EXPAND_FACTOR*i-1个bucket内
	for i := 0; i < len(m.buckets); i = i + 4 {
		originHead := m.buckets[i].head
		m.buckets[i].head = nil
		m.buckets[i].count = 0
		for current := originHead; current != nil; {
			bucketIndex := current.hashVal >> (64 - m.bucketsCountBit)

			newNode := current
			current = current.GetNext()

			newNode.nextPointer = nil
			if m.buckets[bucketIndex].head != nil {
				newNode.nextPointer = m.buckets[bucketIndex].head
				m.buckets[bucketIndex].head = newNode
			} else {
				m.buckets[bucketIndex].head = newNode
			}
			m.buckets[bucketIndex].count++
		}
	}
	//endTime := time.Now()
	//fmt.Printf("after expand resizethreshold is %s, and count is %s, and buckets count is %s, and bucket capacity is %s \n", m.expandThreshold, m.count, len(m.buckets), m.bucketsCapacity)
	//fmt.Printf("time for expand:%s ============ \n", endTime.Sub(startTime))
}

func (m *Lmap) reduce() {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.count >= m.reduceThreshold {
		return
	}

	currentBucketsCapacity := int32(len(m.buckets))
	newBucketsCapacity := currentBucketsCapacity

	if currentBucketsCapacity/REDUCE_FACTOR <= DEFAULT_BUCKETS_CAPACITY {
		newBucketsCapacity = DEFAULT_BUCKETS_CAPACITY
	} else {
		newBucketsCapacity = currentBucketsCapacity / REDUCE_FACTOR
	}

	if newBucketsCapacity == currentBucketsCapacity {
		return
	}

	m.expandThreshold = int32(decimal.NewFromInt32(newBucketsCapacity).Mul(decimal.NewFromInt32(NODES_PER_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(EXPAND_THRES_FACTOR)).IntPart())
	m.reduceThreshold = int32(decimal.NewFromInt32(newBucketsCapacity).Mul(decimal.NewFromFloat32(REDUCE_THRES_FACTOR)).IntPart())
	atomic.AddUint64(&m.bucketsCountBit, ^uint64(math.Log2(float64(REDUCE_FACTOR))-1))

	//将第i个bucket内的nodes合并
	for i := 0; i < len(m.buckets); i++ {
		originHead := m.buckets[i].head
		m.buckets[i].head = nil
		m.buckets[i].count = 0
		for current := originHead; current != nil; {
			bucketIndex := current.hashVal >> (64 - m.bucketsCountBit)

			tempNode := current
			current = current.GetNext()

			tempNode.nextPointer = nil
			if m.buckets[bucketIndex].head != nil {
				tempNode.nextPointer = m.buckets[bucketIndex].head
				m.buckets[bucketIndex].head = tempNode
			} else {
				m.buckets[bucketIndex].head = tempNode
			}
			m.buckets[bucketIndex].count++
		}
	}

	for j := 0; j < len(m.buckets); j = j + 4 {
		m.buckets[j/REDUCE_FACTOR] = m.buckets[j]
	}

	m.buckets = m.buckets[:newBucketsCapacity]
}

func (m *Lmap) createBuckets(bucketsSize int32) (buckets []*lbucket) {
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

func (m *Lmap) getLBucket(hashKey uint64, buckets []*lbucket) (b *lbucket) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.getLBucketWithoutLock(hashKey, buckets)
}

func (m *Lmap) getLBucketWithoutLock(hashKey uint64, buckets []*lbucket) (b *lbucket) {
	return buckets[hashKey>>(64-m.bucketsCountBit)]
}
