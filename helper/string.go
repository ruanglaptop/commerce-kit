package helper

import "fmt"

// CleansingString cleanse string from character " and \
func CleansingString(s string) string {
	resultInRune := []rune{}
	for _, c := range s {
		if fmt.Sprintf("%c", c) != fmt.Sprintf(`"`) && fmt.Sprintf("%c", c) != fmt.Sprintf(`\`) {
			resultInRune = append(resultInRune, c)
		}
	}

	var result string
	for _, c := range resultInRune {
		result = result + string(c)
	}

	return result
}
