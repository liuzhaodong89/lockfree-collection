package main

import (
	"fmt"
	"github.com/dchest/siphash"
	"github.com/dgryski/go-sip13"
	_map "liuzhaodong.com/lockfree-collection/map"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

func main() {
	//lm := _map.New()
	//lm.Set("1", "1")
	//fmt.Println("_________________")
	//fmt.Println(lm.Get("1"))
	//lm.Set("2", "2")
	//fmt.Println(lm.Get("2"))
	//lm.Del("1")
	//fmt.Println(lm.Get("1"))
	//lm.Set("3", "3")
	//fmt.Println(lm.Get("3"))
	//fmt.Println(lm.Get("4"))
	//lm.Set("1", "67")
	//fmt.Println(lm.Get("1"))
	//fmt.Printf("_________________")

	fmt.Println("*************************")
	LOOPCOUNT := 100000
	testMap := _map.New()
	wa2 := sync.WaitGroup{}
	wa2.Add(LOOPCOUNT)
	stm := time.Now()

	for x := 0; x < LOOPCOUNT; x++ {
		go func() {
			testMap.Set(fmt.Sprintf("%v", x), "testval")
			//testMap.Set(x, "testVal")
			wa2.Done()
		}()
	}

	wa2.Wait()
	etm := time.Now()
	fmt.Printf("Time:  %s \n", etm.Sub(stm))

	//var seed1, seed2 uint64 = 0, 0
	//
	//binary.Read(rand.Reader, binary.BigEndian, &seed1)
	//binary.Read(rand.Reader, binary.BigEndian, &seed2)
	//
	//startTime := time.Now()
	//for x := 0; x < LOOPCOUNT; x++ {
	//	testDchestSipHash(seed1, seed2, fmt.Sprintf("%v", x))
	//}
	//stageTime := time.Now()
	//fmt.Printf("First: %s \n", stageTime.Sub(startTime))
	//for y := 0; y < LOOPCOUNT; y++ {
	//	testDgryskiSipHash(seed1, seed2, fmt.Sprintf("%v", y))
	//}
	//endTime := time.Now()
	//fmt.Printf("Second: %s", endTime.Sub(stageTime))
}

func testDchestSipHash(k0, k1 uint64, p string) uint64 {
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&p))
	sh := reflect.SliceHeader{
		Data: hdr.Data,
		Len:  hdr.Len,
		Cap:  hdr.Len,
	}
	return siphash.Hash(k0-1, k1, *(*[]byte)(unsafe.Pointer(&sh)))
}

func testDgryskiSipHash(k0, k1 uint64, p string) uint64 {
	return sip13.Sum64Str(k0, k1, p)
}
