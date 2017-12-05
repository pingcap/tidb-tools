package base62

import "math"

const ary = 62

// Encode encodes m to a string
func Encode(m int64, length int64) string {
	arr := make([]int64, length)
	i := 0
	for m != 0 {
		res := m % ary
		arr[i] = res
		i++
		m /= ary
	}

	return encodeCore(arr, length)
}

func encodeCore(arr []int64, length int64) string {
	var result string
	var value int64
	for _, v := range arr {
		if v < 0 {
			panic("invalid number")
		} else if v < 10 {
			value = v + '0'
		} else if v < 36 {
			value = v + 'A' - 10
		} else {
			value = v + 'a' - 36
		}

		result += string(value)
	}

	return string(result)
}

// Decode decodes a string to a int64
func Decode(str string) int64 {
	var value float64
	var sum float64

	for i, v := range str {
		if v < '0' {
			panic("invalid rune")
		} else if v <= '9' {
			value = float64(v - '0')
		} else if v <= 'Z' {
			value = float64(v - 'A' + 10)
		} else {
			value = float64(v - 'a' + 36)
		}
		sum += value * math.Pow(float64(ary), float64(i))
	}

	return int64(sum)
}
