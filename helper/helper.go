package helper

import "fmt"

func translateDay(date string) string {
	day := date[0:3]
	switch day {
	case "Mon":
		return fmt.Sprintf("%s%s", "Senin", date[3:])
	case "Tue":
		return fmt.Sprintf("%s%s", "Selasa", date[3:])
	case "Wed":
		return fmt.Sprintf("%s%s", "Rabu", date[3:])
	case "Thu":
		return fmt.Sprintf("%s%s", "Kamis", date[3:])
	case "Fri":
		return fmt.Sprintf("%s%s", "Jumat", date[3:])
	case "Sat":
		return fmt.Sprintf("%s%s", "Sabtu", date[3:])
	default:
		return fmt.Sprintf("%s%s", "Minggu", date[3:])
	}
}
