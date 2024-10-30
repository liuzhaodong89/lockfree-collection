package _map

import (
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

type lbucket struct {
	lock  sync.RWMutex
	count int32
	head  *lnode
}

func NewBucket() *lbucket {
	return &lbucket{
		count: 0,
		head:  nil,
	}
}

func (b *lbucket) Get(key interface{}, hashkey uint64) (value interface{}, exist bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	_, node, found := b.find(key, hashkey)
	if !found {
		return nil, false
	}

	return *(*interface{})(node.GetValueAtomically()), found
}

func (b *lbucket) Set(key interface{}, hashkey uint64, value interface{}) (insert int32, success bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	_, node, found := b.find(key, hashkey)
	if found {
		return 0, b.update(key, hashkey, value, node)
	}

	return 1, b.insert(key, hashkey, value, node)
}

func (b *lbucket) Del(key interface{}, hashkey uint64) (success bool) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if key == nil {
		return false
	}

	return b.delete(key, hashkey)
}

func (b *lbucket) find(key interface{}, hashkey uint64) (parent *lnode, current *lnode, exist bool) {
	if nil == key {
		return nil, nil, false
	}

	parent = nil
	count := 0
	if nil != b.head {
		for current := b.head; current != nil; current = current.GetNext() {
			if hashkey == current.GetHash() {
				if *(*interface{})(current.keyPointer) == key {
					//if count < 2 {
					//	fmt.Printf("bucket too long.yes \n")
					//}
					return parent, current, true
				}
			}
			parent = current
			count++
		}
	}
	//if count < 2 {
	//	fmt.Printf("bucket too long.no.%s \n", count)
	//}
	return nil, nil, false
}

func (b *lbucket) insert(key interface{}, hashkey uint64, value interface{}, current *lnode) (success bool) {
	newNode := lnode{
		hashVal:    hashkey,
		keyPointer: unsafe.Pointer(&key),
		valPointer: unsafe.Pointer(&value),
	}

	if current != nil {
		currentNext := current.GetNext()
		b1 := newNode.UpdateNextPointerWithCAS(unsafe.Pointer(newNode.GetNext()), currentNext)
		if !b1 {
			return false
		}

		addResult := current.UpdateNextPointerWithCAS(unsafe.Pointer(currentNext), &newNode)
		if addResult {
			atomic.AddInt32(&b.count, 1)
		} else {
			newNode.UpdateNextPointerWithCAS(unsafe.Pointer(currentNext), nil)
		}
		return addResult
	} else {
		head := b.head
		if b.head != nil {
			atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&newNode.nextPointer)), nil, unsafe.Pointer(head))
		}
		addResult := atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&b.head)), unsafe.Pointer(head), unsafe.Pointer(&newNode))
		if addResult {
			atomic.AddInt32(&b.count, 1)
		} else {
			newNode.UpdateNextPointerWithCAS(unsafe.Pointer(head), nil)
		}
		return addResult
	}
	return false
}

func (b *lbucket) update(key interface{}, hashkey uint64, value interface{}, node *lnode) (success bool) {
	if key == nil {
		return false
	}

	if node.GetHash() == hashkey {
		return node.UpdateValueWithCAS(node.GetValueAtomically(), value)
	}
	return false
}

func (b *lbucket) delete(key interface{}, hashkey uint64) (success bool) {
	parent, current, exist := b.find(key, hashkey)

	if !exist {
		return true
	}

	var newNext *lnode = nil
	if current != nil {
		newNext = current.GetNext()
	}

	delResult := false
	if parent == nil {
		delResult = atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&b.head)), unsafe.Pointer(current), unsafe.Pointer(newNext))
	} else {
		delResult = parent.UpdateNextPointerWithCAS(unsafe.Pointer(current), newNext)
	}

	if delResult {
		current.UpdateNextPointerWithCAS(unsafe.Pointer(newNext), nil)
		current = nil
		atomic.AddInt32(&b.count, -1)
	}
	return delResult
}

func (b *lbucket) Size() (count uint32) {
	return count
}

func (b *lbucket) Split(hashes []uint64) (headNodes []*lnode) {
	if b.head == nil {
		return nil
	}
	if hashes != nil {
		sort.Slice(hashes, func(i, j int) bool {
			return i < j
		})

		hashIndex := 0
		node := b.head
		var parent *lnode = nil
		for ; node.nextPointer != nil; node = node.GetNext() {
			if node.hashVal >= hashes[hashIndex] && (parent == nil || parent != nil && parent.hashVal < hashes[hashIndex]) {
				headNodes = append(headNodes, node)
				hashIndex++

				if hashIndex >= len(hashes) {
					break
				}
			}
			parent = node
		}
	}
	return headNodes
}
