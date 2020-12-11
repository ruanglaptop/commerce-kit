package helper

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/payfazz/commerce-kit/notif"
)

// ConsumeEventFromFile consume event from file with return idempotentId, object, and event bytes
func ConsumeEventFromFile(fileName string, topic string, notifier notif.Notifier) (string, string, []byte) {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("[ConsumeEventFromFile] Error while readEventFromFile: %v", err)
		notifier.Notify(fmt.Sprintf("[ConsumeEventFromFile] Error while readEventFromFile: %v", err))
		return "", "", nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string

	for scanner.Scan() {
		text = append(text, scanner.Text())
	}

	for _, message := range text {
		chunk := strings.Split(message, "-")
		if len(chunk) > 3 && chunk[1] == topic {
			return chunk[0], chunk[2], []byte(chunk[3])
		}
	}

	return "", "", nil
}

// IsEventExistInFile check whether the event is exist in file
func IsEventExistInFile(fileName string, topic string, idempotentID string, notifier notif.Notifier) bool {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("[IsEventExistInFile] Error while readEventFromFile: %v", err)
		notifier.Notify(fmt.Sprintf("[IsEventExistInFile] Error while readEventFromFile: %v", err))
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string

	for scanner.Scan() {
		text = append(text, scanner.Text())
	}

	for _, message := range text {
		chunk := strings.Split(message, "-")
		if len(chunk) > 3 && chunk[1] == topic && chunk[0] == idempotentID {
			return true
		}
	}

	return false
}

// AcknowledgeEventFromFile acknowledge / remove event from file by idempotentId
func AcknowledgeEventFromFile(fileName string, topic string, idempotentID string, notifier notif.Notifier) {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("Error while AcknowledgeEventFromFile: %v", err)
		notifier.Notify(fmt.Sprintf("[AcknowledgeEventFromFile] Error while AcknowledgeEventFromFile: %v", err))
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string

	for scanner.Scan() {
		text = append(text, scanner.Text())
	}

	var newMessages string
	distinctFlagMap := map[string]map[string]bool{}
	for idx, message := range text {
		chunk := strings.Split(message, "-")
		if len(chunk) > 3 {
			if distinctFlagMap[chunk[0]] == nil {
				distinctFlagMap[chunk[0]] = map[string]bool{}
			}

			if distinctFlagMap[chunk[0]][chunk[1]] == false {
				distinctFlagMap[chunk[0]][chunk[1]] = true

				if chunk[0] != idempotentID || chunk[1] != topic {
					newMessages = newMessages + fmt.Sprintf("%s-%s-%s-%v", chunk[0], chunk[1], chunk[2], chunk[3])
					if idx != len(text)-1 {
						newMessages = newMessages + "\n"
					}
				}
			}
		}
	}

	file.Close()
	os.Remove(fileName)

	file, err = os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error while AcknowledgeEventFromFile on writing event to file: %v", err)
		notifier.Notify(fmt.Sprintf("[AcknowledgeEventFromFile] Error while AcknowledgeEventFromFile on writing event to file: %v", err))
	}

	// implement logger to overcome race condition issue, since log have its own mutex process
	logger := log.New(file, "", 0)
	logger.Output(2, newMessages)
	file.Close()
}
