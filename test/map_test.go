package main

import (
	"fmt"
	_map "github.com/liuzhaodong89/lockfree-collection/map"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func generateWithCap(n int) []int {
	rand.Seed(time.Now().UnixNano())
	nums := make([]int, 0, n)
	for i := 0; i < n; i++ {
		nums = append(nums, rand.Int())
	}
	return nums
}

// 基准free map写入测试
func BenchmarkWriteFreeMap(b *testing.B) {
	b.N = 10000000
	m := _map.New()
	// b.N 是测试运行的次数，它会自动增大，以确保结果准确
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("%d", i), "test value")
	}
}

// 基准sync map 写入测试
func BenchmarkWriteSyncMap(b *testing.B) {
	b.N = 10000000
	// 初始化 sync.Map
	var m sync.Map
	// b.N 是测试运行的次数，它会自动增大，以确保结果准确
	for i := 0; i < b.N; i++ {
		m.Store(fmt.Sprintf("%d", i), "test value")
	}
}

// 基准sync map 写入测试
func BenchmarkWriteBaseMap(b *testing.B) {
	b.N = 10000000
	// 初始化 sync.Map
	m := make(map[string]string)
	// b.N 是测试运行的次数，它会自动增大，以确保结果准确
	for i := 0; i < b.N; i++ {
		m[fmt.Sprintf("%d", i)] = "test value"
	}
}

// 基准free map读取测试
func BenchmarkReadFreeMap(b *testing.B) {
	// 先初始化数据
	m := _map.New()
	for i := 0; i < 10000000; i++ {
		m.Set(fmt.Sprintf("%d", i), fmt.Sprintf("value%d", i))
	}

	// 开始基准测试，读取数据
	b.N = 10000000
	b.ResetTimer() // 重置计时器，避免初始化时间的影响
	for i := 0; i < b.N; i++ {
		// 模拟读取数据
		_, _ = m.Get(fmt.Sprintf("%d", i%10000000))
	}
}

// 基准sync.Map读取测试
func BenchmarkReadSyncMap(b *testing.B) {
	// 初始化sync.Map
	var m sync.Map
	for i := 0; i < 10000000; i++ {
		m.Store(fmt.Sprintf("%d", i), fmt.Sprintf("value%d", i))
	}

	// 开始基准测试，读取数据
	b.N = 10000000
	b.ResetTimer() // 重置计时器，避免初始化时间的影响
	for i := 0; i < b.N; i++ {
		// 模拟读取数据
		_, _ = m.Load(fmt.Sprintf("%d", i%10000000))
	}
}

// 基准map读取测试
func BenchmarkReadBaseMap(b *testing.B) {
	// 初始化普通map
	m := make(map[string]string)
	for i := 0; i < 10000000; i++ {
		m[fmt.Sprintf("%d", i)] = fmt.Sprintf("value%d", i)
	}

	// 开始基准测试，读取数据
	b.N = 10000000
	b.ResetTimer() // 重置计时器，避免初始化时间的影响
	for i := 0; i < b.N; i++ {
		// 模拟读取数据
		_ = m[fmt.Sprintf("%d", i%10000000)]
	}
}
