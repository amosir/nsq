package util

import (
	"math/rand"
)

// 用于生成不重复的随机数，其中 quantity 表示随机数个数， maxval 表示随机数最大值
// 洗牌算法: https://zhuanlan.zhihu.com/p/73147939
func UniqRands(quantity int, maxval int) []int {
	if maxval < quantity {
		quantity = maxval
	}

	// 将切片中的元素初始化为从0到maxval-1的连续整数
	intSlice := make([]int, maxval)
	for i := 0; i < maxval; i++ {
		intSlice[i] = i
	}

	// 生成quantity个不重复的随机数
	for i := 0; i < quantity; i++ {
		// 生成一个介于i和maxval之间的随机数j
		j := rand.Int()%maxval + i
		// 交换intSlice[i]和intSlice[j]的值
		intSlice[i], intSlice[j] = intSlice[j], intSlice[i]
		maxval--

	}
	return intSlice[0:quantity]
}
