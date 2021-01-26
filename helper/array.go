package helper

// RemoveIndexInArray remove an item from slice / array
func RemoveIndexInArray(s []int, i int) []int {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}
