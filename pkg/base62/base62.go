package base62

// base charset [0-9A-Za-z]
const base int64 = 62

// Encode encodes m to a string
func Encode(m int64, length int) string {
	bits := make([]int64, length)
	strs := make([]byte, length)
	
	for i := 0; m != 0 && i < length; i++ {
		res := m % base
		bits[i] = res
		m /= base
	}
	
	for i, v := range bits {
		if v < 10 {
			strs[i] = byte(v) + '0'
		} else if v < 36 {
			strs[i] = byte(v-10) + 'A'
		} else {
			strs[i] = byte(v-36) + 'a'
		}
	}

	return string(strs)
}

// Decode decodes a string to a int64
func Decode(str string) int64 {
	var magnitude int64 = 1
	var origin int64

	for _, v := range str {
		if v <= '9' {
			origin += int64(v - '0')*magnitude
		} else if v <= 'Z' {
			origin += int64(v - 'A' + 10)*magnitude
		} else {
			origin += int64(v - 'a' + 36)*magnitude
		}
		magnitude = magnitude*base
	}

	return origin
}
