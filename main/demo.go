package main

import (
	"fmt"
	"liuzhaodong.com/lockfree-collection/map"
)

func main() {
	lm := _map.New()
	lm.Set("1", "1")
	fmt.Println("_________________")
	fmt.Println(lm.Get("1"))
	lm.Set("2", "2")
	fmt.Println(lm.Get("2"))
	lm.Del("1")
	fmt.Println(lm.Get("1"))
	lm.Set("3", "3")
	fmt.Println(lm.Get("3"))
	fmt.Println(lm.Get("4"))
	lm.Set("1", "67")
	fmt.Println(lm.Get("1"))
	fmt.Printf("_________________")
}
