package file

import (
	"bufio"
	"log"
	"os"
	"strings"
)

// AppendToFile append text to file after new line
func AppendToFile(fileName string, newText string) error {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	// workdir, _ := os.Getwd()
	// log.Printf("\nWork Dir: %v\n", workdir)
	// err := filepath.Walk(".",
	// 	func(path string, info os.FileInfo, err error) error {
	// 		if err != nil {
	// 			return err
	// 		}
	// 		log.Println(path, info.Size())
	// 		return nil
	// 	})
	// if err != nil {
	// 	log.Println(err)
	// }

	file, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string

	for scanner.Scan() {
		text = append(text, scanner.Text())
	}

	isExist := false
	for _, line := range text {
		if newText == line {
			isExist = true
		}
	}

	if isExist {
		return nil
	}

	text = append(text, newText)

	file.Close()

	file, err = os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// implement logger to overcome race condition issue, since log have its own mutex process
	logger := log.New(file, "", 0)
	logger.Output(2, strings.Join(text, "\n"))
	file.Close()

	return nil
}

// WriteToFile write text to file
func WriteToFile(fileName string, text string) error {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// implement logger to overcome race condition issue, since log have its own mutex process
	logger := log.New(file, "", 0)
	logger.Output(2, text)
	file.Close()

	return nil
}

// ReadFromFile read text to file return array of string (array represent the line number in file)
func ReadFromFile(fileName string) ([]string, error) {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string

	for scanner.Scan() {
		text = append(text, scanner.Text())
	}

	file.Close()

	return text, nil
}
