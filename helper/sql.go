package helper

import "strconv"

//SQLIntSeq for query in when type is array integer
func SQLIntSeq(ns []int) string {
	if len(ns) == 0 {
		return ""
	}
	// Appr. 3 chars per num plus the comma.
	estimate := len(ns) * 4
	b := make([]byte, 0, estimate)
	// Or simply
	for _, n := range ns {
		b = strconv.AppendInt(b, int64(n), 10)
		b = append(b, ',')
	}
	b = b[:len(b)-1]
	return string(b)
}
