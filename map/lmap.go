package _map

import (
	"crypto/rand"
	"encoding/binary"
	"github.com/liuzhaodong89/lockfree-collection/common"
	"github.com/shopspring/decimal"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

type lmap struct {
	count           int32
	bucketsCapacity int32
	buckets         []*lbucket
	bucketsCountBit uint64

	resizeThreshold int32
	lock            sync.RWMutex
	seed1, seed2    uint64
}

const MAX_BUCKETS_CAPACITY int32 = math.MaxInt32
const DEFAULT_BUCKETS_CAPACITY int32 = 2048
const EXPAND_THRES_FACTOR float32 = 0.4
const EXPAND_FACTOR = 4
const NODES_PER_BUCKET_CAPACITY = 10

func New() (m *lmap) {
	lm := lmap{
		resizeThreshold: int32(decimal.NewFromInt32(DEFAULT_BUCKETS_CAPACITY).Mul(decimal.NewFromInt(NODES_PER_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(EXPAND_THRES_FACTOR)).IntPart()),
		bucketsCapacity: DEFAULT_BUCKETS_CAPACITY,
		count:           0,
		buckets:         make([]*lbucket, DEFAULT_BUCKETS_CAPACITY),
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

	m.bucketsCapacity = newBucketsCapacity
	m.resizeThreshold = int32(decimal.NewFromInt32(newBucketsCapacity).Mul(decimal.NewFromInt32(DEFAULT_BUCKETS_CAPACITY)).Mul(decimal.NewFromFloat32(EXPAND_THRES_FACTOR)).IntPart())
	nBuckets := m.buckets

	//expand_bit := uint64(math.Log2(float64(EXPAND_FACTOR)))
	//for i := 0; i < int(expand_bit); i++ {
	//	nBuckets = append(nBuckets, nBuckets...)
	//}

	for i := 1; i < EXPAND_FACTOR; i++ {
		nBuckets = append(nBuckets, m.buckets...)
	}
	m.bucketsCountBit = uint64(math.Log2(float64(len(nBuckets))))

	//将原先第i个bucket，移动至第EXPAND_FACTOR*i个位置
	for i := 0; i < len(nBuckets); i++ {
		if i%EXPAND_FACTOR == 0 {
			nBuckets[i].head = m.buckets[i/EXPAND_FACTOR].head
		} else {
			nBuckets[i].head = nil
		}
	}

	//将第EXPAND_FACTOR*i个bucket内的nodes重新切分，分配到EXPAND_FACTOR*(i-1)+1到EXPAND_FACTOR*i-1个bucket内
	for i := 0; i < len(nBuckets); i = (i + 1) * EXPAND_FACTOR {
		originHead := nBuckets[i].head
		nBuckets[i].head = nil
		for current := originHead; current != nil; {
			bucketIndex := current.hashVal >> (64 - m.bucketsCountBit)

			newNode := current
			current = current.GetNext()
			if nBuckets[bucketIndex].head != nil {
				atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&newNode.nextPointer)), nil, unsafe.Pointer(nBuckets[bucketIndex].head))
				atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&nBuckets[bucketIndex].head)), unsafe.Pointer(nBuckets[bucketIndex].head), unsafe.Pointer(&newNode))
			} else {
				nBuckets[bucketIndex].head = newNode
			}
		}
	}
	m.buckets = nBuckets

	//oldCap := m.bucketsCapacity
	//
	//if oldCap > 0 {
	//	if oldCap >= MAX_BUCKETS_CAPACITY {
	//		m.resizeThreshold = MAX_BUCKETS_CAPACITY
	//	} else {
	//		newCap := oldCap
	//		if oldCap >= MAX_BUCKETS_CAPACITY/EXPAND_FACTOR {
	//			newCap = MAX_BUCKETS_CAPACITY
	//		} else {
	//			newCap = oldCap * EXPAND_FACTOR
	//		}
	//
	//		newThres := int32(decimal.NewFromInt32(newCap).Mul(decimal.NewFromInt(NODES_PER_BUCKET_CAPACITY)).Mul(decimal.NewFromFloat32(EXPAND_THRES_FACTOR)).IntPart())
	//		m.resizeThreshold = newThres
	//		m.bucketsCapacity = newCap
	//		//stageTime1 := time.Now()
	//		//fmt.Printf("stage time for calculate new params:%s \n", stageTime1.Sub(startTime))
	//
	//		newBuckets := m.createBuckets(newCap)
	//
	//		var fi int32 = 0
	//		//newBuckets := make([]*lbucket, 0)
	//		for ; fi < EXPAND_FACTOR; fi++ {
	//			newBuckets = append(newBuckets, m.buckets...)
	//		}
	//
	//		//stageTime2 := time.Now()
	//		//fmt.Printf("stage time for create new buckets slice:%s \n", stageTime2.Sub(stageTime1))
	//		m.bucketsCountBit = uint64(math.Log2(float64(len(newBuckets))))
	//		//endTime1 := time.Now()
	//		//fmt.Printf("time for expand buckets: %s \n", endTime1.Sub(startTime))
	//
	//		//rehash
	//		if newBuckets != nil {
	//			for i := 0; i < len(newBuckets); i++ {
	//				if i%EXPAND_FACTOR != 0 {
	//					//newBuckets[i].count = 0
	//					newBuckets[i].head = nil
	//				}
	//			}
	//			//endTime2 := time.Now()
	//			//fmt.Printf("time for copy buckets: %s \n", endTime2.Sub(endTime1))
	//
	//			hashesSlice := make([]uint64, EXPAND_FACTOR-1)
	//			for j := 0; j < len(newBuckets); j = (j + 1) * EXPAND_FACTOR {
	//				hashesSlice = hashesSlice[0:0]
	//				for x := 1; x < EXPAND_FACTOR; x++ {
	//					hashesSlice = append(hashesSlice, uint64(j+x)<<(64-m.bucketsCountBit))
	//				}
	//				nodes := newBuckets[j].Split(hashesSlice)
	//				if nodes != nil {
	//					for y := 0; y < len(nodes); y++ {
	//						newBuckets[j+y].head = nodes[y]
	//					}
	//				}
	//			}
	//			//endTime3 := time.Now()
	//			//fmt.Printf("time for move nodes: %s \n", endTime3.Sub(endTime2))
	//		}
	//		m.buckets = newBuckets
	//	}
	//}
	////endTime := time.Now()
	////fmt.Printf("time for resize:%s ============  and count is %v ,and threshold is %v \n", endTime.Sub(startTime), m.count, m.resizeThreshold)
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
