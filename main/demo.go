package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/dchest/siphash"
	"github.com/dgryski/go-farm"
	"github.com/dgryski/go-sip13"
	"github.com/go-faster/city"
	_map "github.com/liuzhaodong89/lockfree-collection/map"
	"github.com/minio/highwayhash"
	"github.com/tenfyzhong/cityhash"
	cityhash2 "github.com/zentures/cityhash"
	"hash"
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

	//var int string = "test"
	//fmt.Printf(int)

	//fmt.Println("*************************")
	LOOPCOUNT := 1000000
	testMap := _map.New()
	wa2 := sync.WaitGroup{}
	wa2.Add(LOOPCOUNT)
	stm := time.Now()

	for x := 0; x < LOOPCOUNT; x++ {
		go func(tmp int) {
			s := testMap.Set(fmt.Sprintf("%v", tmp), "testval")
			if !s {
				s2 := testMap.Set(tmp, "testval")
				//	if !s2 {
				//		s3 := testMap.Set(tmp, "testval")
				//		if !s3 {
				//			s4 := testMap.Set(tmp, "testval")
				//			if !s4 {
				//				s5 := testMap.Set(tmp, "testval")
				if !s2 {
					fmt.Printf("Failed Insert: %v \n", tmp)
				}
				//			}
				//		}
				//	}
			}
			wa2.Done()
		}(x)
	}
	v, _ := testMap.Get("1456")
	fmt.Printf("Test:%v \n", v)

	wa2.Wait()
	etm := time.Now()
	fmt.Printf("并行 Time:  %s \n", etm.Sub(stm))

	for y := 0; y < LOOPCOUNT; y++ {
		go func(tmp int) {
			_, e := testMap.Get(fmt.Sprintf("%v", tmp))
			if !e {
				//fmt.Printf("Alert! key is %v \n", tmp)
			}
		}(y)
	}

	testMap2 := _map.New()
	stm1 := time.Now()
	for j := 0; j < LOOPCOUNT; j++ {
		testMap2.Set(fmt.Sprintf("%v", j), "TestVal")
	}
	etm1 := time.Now()
	v1, _ := testMap2.Get("11456")
	fmt.Printf("Test2: %v \n", v1)
	fmt.Printf("串行 Time:  %s \n", etm1.Sub(stm1))

	rstm1 := time.Now()
	for ri := 0; ri < LOOPCOUNT; ri++ {
		_, ok := testMap.Get(fmt.Sprintf("%v", ri))
		if !ok {
			fmt.Printf("Failed to get key: %v \n", ri)
		}
	}
	retm1 := time.Now()
	fmt.Printf("串行读 Time:  %s \n", retm1.Sub(rstm1))

	rstm2 := time.Now()
	rwa := sync.WaitGroup{}
	rwa.Add(LOOPCOUNT)
	for rj := 0; rj < LOOPCOUNT; rj++ {
		go func(tmp int) {
			_, ok := testMap2.Get(fmt.Sprintf("%v", tmp))
			if !ok {
				fmt.Printf("Parallel failed to get key: %v \n", tmp)
			}
			rwa.Done()
		}(rj)
	}
	rwa.Wait()
	retm2 := time.Now()
	fmt.Printf("并行读 Time: %s \n", retm2.Sub(rstm2))

	testSyncMap := sync.Map{}
	//wa3 := sync.WaitGroup{}
	//wa3.Add(LOOPCOUNT)
	sstm := time.Now()

	for y := 0; y < LOOPCOUNT; y++ {
		//go func() {
		testSyncMap.Store(fmt.Sprint("%v", y), "testVal")
		//wa3.Done()
		//}()
	}

	//wa3.Wait()
	estm := time.Now()
	fmt.Printf("Sync map 写入  %s \n", estm.Sub(sstm))

	rstm3 := time.Now()

	for y := 0; y < LOOPCOUNT; y++ {
		//go func() {
		testSyncMap.Load(fmt.Sprint("%v", y))
		//wa3.Done()
		//}()
	}

	//wa3.Wait()
	retm3 := time.Now()
	fmt.Printf("Sync map 读取  %s \n", retm3.Sub(rstm3))

	//for x := 0; x < LOOPCOUNT; x++ {
	//	_, e := testMap2.Get(fmt.Sprintf("%v", x))
	//	if !e {
	//		fmt.Printf("Alert! key is %v \n", x)
	//	}
	//}
	//for x := 0; x < LOOPCOUNT; x++ {
	//	s := testMap2.Del(fmt.Sprintf("%v", x))
	//	if !s {
	//		fmt.Printf("Delete Failed! key is %v \n", x)
	//	}
	//}
	//fmt.Printf("******************** Key:999, string: ")
	//fmt.Println(testMap.Get("999"))
	//fmt.Printf("******************** Key:999, int: ")
	//fmt.Println(testMap.Get(999))
	//fmt.Printf("******************** Key:999, float: ")
	//fmt.Println(testMap.Get(float64(999)))

	var seed1, seed2 uint64 = 0, 1

	binary.Read(rand.Reader, binary.BigEndian, &seed1)
	binary.Read(rand.Reader, binary.BigEndian, &seed2)

	//startTime := time.Now()
	//for x := 0; x < LOOPCOUNT; x++ {
	//	testDchestSipHash(seed1, seed2, fmt.Sprintf("%v", x))
	//}
	//stageTime := time.Now()
	//fmt.Printf("First Sip: %s \n", stageTime.Sub(startTime))
	//for y := 0; y < LOOPCOUNT; y++ {
	//	testDgryskiSipHash(seed1, seed2, fmt.Sprintf("%v", y))
	//}
	//endTime := time.Now()
	//fmt.Printf("Second Sip: %s \n", endTime.Sub(stageTime))
	//
	//key, _ := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000") // use your own key here
	//p, _ := hex.DecodeString("12345")
	//hwStartTime := time.Now()
	//for hwIndex := 0; hwIndex < LOOPCOUNT; hwIndex++ {
	//	testHighwayHash(key, p)
	//}
	//hwTime := time.Now()
	//fmt.Printf("Highway: %s \n", hwTime.Sub(hwStartTime))
	//
	////s, _ := hex.DecodeString("123456123456123456123456123456123456123456123456")
	//for ctIndex := 0; ctIndex < LOOPCOUNT; ctIndex++ {
	//	p := fmt.Sprintf("%s", ctIndex)
	//	testCityHash1([]byte(p))
	//}
	//ctTime := time.Now()
	//fmt.Printf("City: %s \n", ctTime.Sub(hwTime))
	//
	//for ct2Index := 0; ct2Index < LOOPCOUNT; ct2Index++ {
	//	//s, _ = hex.DecodeString("123456123456123456123456123456123456123456123456")
	//	p := fmt.Sprintf("%s", ct2Index)
	//	testCityHash2([]byte(p))
	//}
	//ct2Time := time.Now()
	//fmt.Printf("City2: %s \n", ct2Time.Sub(ctTime))
	//
	//for ct3Index := 0; ct3Index < LOOPCOUNT; ct3Index++ {
	//	//s, _ = hex.DecodeString("123456123456123456123456123456123456123456123456")
	//	p := fmt.Sprintf("%s", ct3Index)
	//	hdr := (*reflect.StringHeader)(unsafe.Pointer(&p))
	//	sh := reflect.SliceHeader{
	//		Data: hdr.Data,
	//		Len:  hdr.Len,
	//		Cap:  hdr.Len,
	//	}
	//	testCityHash3(*(*[]byte)(unsafe.Pointer(&sh)), 0, seed1, seed2)
	//}
	//ct3Time := time.Now()
	//fmt.Printf("City3: %s \n", ct3Time.Sub(ct2Time))
	//
	//d := xxhash.New()
	//for xxIndex := 0; xxIndex < LOOPCOUNT; xxIndex++ {
	//	//s, _ = hex.DecodeString("123456123456123456123456123456123456123456123456")
	//	p := fmt.Sprintf("%s", xxIndex)
	//	testXXHash([]byte(p), d)
	//}
	//xxTime := time.Now()
	//fmt.Printf("xxHash: %s \n", xxTime.Sub(ct3Time))
	//
	//for farmIndex := 0; farmIndex < LOOPCOUNT; farmIndex++ {
	//	//s, _ = hex.DecodeString("123456123456123456123456123456123456123456123456")
	//	p := fmt.Sprintf("%s", farmIndex)
	//	testFarmHash([]byte(p))
	//}
	//farmTime := time.Now()
	//fmt.Printf("farmHash: %s \n", farmTime.Sub(xxTime))
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

func testHighwayHash(key []byte, p []byte) []byte {
	hash, _ := highwayhash.New64(key)
	checkSum := hash.Sum(p)
	return checkSum
}

func testCityHash1(s []byte) uint64 {
	hash64 := cityhash.CityHash64(s)
	return hash64
}

func testCityHash2(s []byte) uint64 {
	result := city.Hash64(s)
	return result
}

func testCityHash3(s []byte, len uint32, seed0, seed1 uint64) uint64 {
	return cityhash2.CityHash64WithSeeds(s, len, seed0, seed1)
}

func testXXHash(s []byte, d hash.Hash64) uint64 {
	d.Reset()
	d.Write(s)
	return d.Sum64()
}

func testFarmHash(s []byte) uint64 {
	return farm.Hash64(s)
}
