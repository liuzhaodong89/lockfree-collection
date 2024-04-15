package _map

import (
	"sync/atomic"
	"unsafe"
)

type lnode struct {
	hashVal     uint64
	keyPointer  unsafe.Pointer
	valPointer  unsafe.Pointer
	nextPointer *lnode
}

func (node *lnode) GetHash() (hash uint64) {
	return node.hashVal
}

func (node *lnode) GetKeyAtomically() (key unsafe.Pointer) {
	return atomic.LoadPointer(&node.keyPointer)
}

func (node *lnode) GetValueAtomically() (value unsafe.Pointer) {
	return atomic.LoadPointer(&node.valPointer)
}

func (node *lnode) GetNext() (next *lnode) {
	return node.nextPointer
}

func (node *lnode) UpdateValueWithCAS(expected unsafe.Pointer, target interface{}) bool {
	return atomic.CompareAndSwapPointer(&node.valPointer, expected, unsafe.Pointer(&target))
}

func (node *lnode) UpdateNextPointerWithCAS(expected unsafe.Pointer, newNode *lnode) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&node.nextPointer)), expected, unsafe.Pointer(newNode))
}

func (node *lnode) IsNilNode() bool {
	return node == nil || node.keyPointer == nil
}
