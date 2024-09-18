package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/dchest/siphash"
	"github.com/dgryski/go-sip13"
	"github.com/go-faster/city"
	_map "github.com/liuzhaodong89/lockfree-collection/map"
	"github.com/minio/highwayhash"
	"github.com/tenfyzhong/cityhash"
	cityhash2 "github.com/zentures/cityhash"
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
			//testMap.Set(float64(x), "testval")
			//testMap.Set(x, "testVal")
			wa2.Done()
		}()
	}

	wa2.Wait()
	etm := time.Now()
	fmt.Printf("Time:  %s \n", etm.Sub(stm))

	var seed1, seed2 uint64 = 0, 0

	binary.Read(rand.Reader, binary.BigEndian, &seed1)
	binary.Read(rand.Reader, binary.BigEndian, &seed2)

	startTime := time.Now()
	for x := 0; x < LOOPCOUNT; x++ {
		testDchestSipHash(seed1, seed2, fmt.Sprintf("%v", x))
	}
	stageTime := time.Now()
	fmt.Printf("First: %s \n", stageTime.Sub(startTime))
	for y := 0; y < LOOPCOUNT; y++ {
		testDgryskiSipHash(seed1, seed2, fmt.Sprintf("%v", y))
	}
	endTime := time.Now()
	fmt.Printf("Second: %s \n", endTime.Sub(stageTime))

	key, _ := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000") // use your own key here
	p, _ := hex.DecodeString("12345")
	hwStartTime := time.Now()
	for hwIndex := 0; hwIndex < LOOPCOUNT; hwIndex++ {
		testHighwayHash(key, p)
	}
	hwTime := time.Now()
	fmt.Printf("Highway: %s \n", hwTime.Sub(hwStartTime))

	s, _ := hex.DecodeString("123456123456123456123456123456123456123456123456")
	for ctIndex := 0; ctIndex < LOOPCOUNT; ctIndex++ {
		testCityHash1(s)
	}
	ctTime := time.Now()
	fmt.Printf("City: %s \n", ctTime.Sub(hwTime))

	for ct2Index := 0; ct2Index < LOOPCOUNT; ct2Index++ {
		testCityHash2(s)
	}
	ct2Time := time.Now()
	fmt.Printf("City2: %s \n", ct2Time.Sub(ctTime))

	for ct3Index := 0; ct3Index < LOOPCOUNT; ct3Index++ {
		testCityHash3(s, 0, seed1, seed2)
	}
	ct3Time := time.Now()
	fmt.Printf("City3: %s \n", ct3Time.Sub(ct2Time))
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
