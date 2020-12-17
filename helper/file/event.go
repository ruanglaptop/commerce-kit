package file

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/payfazz/commerce-kit/helper"
	"github.com/payfazz/commerce-kit/notif"
	"github.com/payfazz/commerce-kit/types"
	"gocloud.dev/pubsub"
)

// EventMirroringToFileService implementor of event mirroring service which stores to file
type EventMirroringToFileService struct {
	serviceName  string
	fileName     string
	pubsubTopics map[string]*pubsub.Topic
	notifier     notif.Notifier
}

// Consume consume event from file with return the first event from file
func (s *EventMirroringToFileService) Consume(ctx *context.Context, topicName string) (*helper.Event, *types.Error) {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.Open(s.fileName)
	if err != nil {
		log.Printf(".EventMirroringToFileService->Consume(): %v", err)
		s.notifier.Notify(fmt.Sprintf(".EventMirroringToFileService->Consume(): %v", err))
		return nil, &types.Error{
			Path:    ".EventMirroringToFileService->Consume()",
			Message: fmt.Sprintf(".EventMirroringToFileService->Consume(): %v", err),
			Error:   fmt.Errorf(".EventMirroringToFileService->Consume(): %v", err),
			Type:    "eventMirroringService-error",
		}
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
		if len(chunk) > 5 && chunk[0] == s.serviceName && chunk[1] == topicName {
			return &helper.Event{
				ServiceName:  chunk[0],
				TopicName:    chunk[1],
				IdempotentID: chunk[2],
				Object:       chunk[3],
				Message:      chunk[4],
			}, nil
		}
	}

	return nil, nil
}

// IsExist check whether the event is exist in file
func (s *EventMirroringToFileService) IsExist(ctx *context.Context, event *helper.Event) bool {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.Open(s.fileName)
	if err != nil {
		log.Printf(".EventMirroringToFileService->IsExist(): %v", err)
		s.notifier.Notify(fmt.Sprintf(".EventMirroringToFileService->IsExist(): %v", err))
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
		if len(chunk) > 5 && chunk[0] == s.serviceName && chunk[1] == event.TopicName && chunk[2] == event.IdempotentID && chunk[3] == event.Object {
			return true
		}
	}

	return false
}

// Acknowledge acknowledge / remove event from file by idempotentId
func (s *EventMirroringToFileService) Acknowledge(ctx *context.Context, event *helper.Event) *types.Error {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.Open(s.fileName)
	if err != nil {
		log.Printf(".EventMirroringToFileService->Acknowledge: %v", err)
		s.notifier.Notify(fmt.Sprintf(".EventMirroringToFileService->Acknowledge: %v", err))
		return &types.Error{
			Path:    ".EventMirroringToFileService->Acknowledge()",
			Message: fmt.Sprintf(".EventMirroringToFileService->Acknowledge(): %v", err),
			Error:   fmt.Errorf(".EventMirroringToFileService->Acknowledge(): %v", err),
			Type:    "eventMirroringService-error",
		}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string

	for scanner.Scan() {
		text = append(text, scanner.Text())
	}

	var newMessages string
	distinctFlagMap := map[string]map[string]map[string]bool{}
	for idx, message := range text {
		chunk := strings.Split(message, "-")
		if len(chunk) > 5 {
			if distinctFlagMap[chunk[0]] == nil {
				distinctFlagMap[chunk[0]] = map[string]map[string]bool{}
			}

			if distinctFlagMap[chunk[0]][chunk[1]] == nil {
				distinctFlagMap[chunk[0]][chunk[1]] = map[string]bool{}
			}

			if distinctFlagMap[chunk[0]][chunk[1]][chunk[2]] == false {
				distinctFlagMap[chunk[0]][chunk[1]][chunk[2]] = true

				if chunk[0] != s.serviceName || chunk[1] != event.TopicName || chunk[2] != event.IdempotentID {
					newMessages = newMessages + fmt.Sprintf("%s-%s-%s-%s-%v", chunk[0], chunk[1], chunk[2], chunk[3], chunk[4])
					if idx != len(text)-1 {
						newMessages = newMessages + "\n"
					}
				}
			}
		}
	}

	file.Close()
	os.Remove(s.fileName)

	file, err = os.OpenFile(s.fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf(".EventMirroringToFileService->Acknowledge: %v", err)
		s.notifier.Notify(fmt.Sprintf(".EventMirroringToFileService->Acknowledge: %v", err))
		return &types.Error{
			Path:    ".EventMirroringToFileService->Acknowledge()",
			Message: fmt.Sprintf(".EventMirroringToFileService->Acknowledge(): %v", err),
			Error:   fmt.Errorf(".EventMirroringToFileService->Acknowledge(): %v", err),
			Type:    "eventMirroringService-error",
		}
	}

	// implement logger to overcome race condition issue, since log have its own mutex process
	logger := log.New(file, "", 0)
	logger.Output(2, newMessages)
	file.Close()

	return nil
}

// Publish publish event with mirroring in file
func (s *EventMirroringToFileService) Publish(ctx *context.Context, params *helper.PublishEventParams) *types.Error {
	params.Metadata["serviceName"] = s.serviceName
	for _, topicName := range params.TopicNames {
		metadata["action"] = topicName

		message := fmt.Sprintf("%s-%s-%s-%s-%v", params.Metadata["serviceName"], params.Metadata["action"], params.Metadata["idempotentId"], params.Metadata["object"], params.Body)

		errFileHandler := AppendToFile(s.fileName, message)
		if errFileHandler != nil {
			log.Printf(".EventMirroringToFileService->Publish(): Error on publishing event (Topic: %s) to file: %v", topicName, &types.Error{
				Path:    ".EventMirroringToFileService->Publish()",
				Message: errFileHandler.Error(),
				Error:   errFileHandler,
				Type:    "eventMirroringService-error",
			})

			errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToFileService->Publish(): Error on publishing event (Topic: %s) to file: %v", topicName, &types.Error{
				Path:    ".EventMirroringToFileService->Publish()",
				Message: errFileHandler.Error(),
				Error:   errFileHandler,
				Type:    "eventMirroringService-error",
			}))
			if errNotification != nil {
				log.Println(errNotification)
			}
		}

		errPubsub := s.pubsubTopics[topicName].Send(
			*ctx, &pubsub.Message{
				Body:     params.Body,
				Metadata: params.Metadata,
			},
		)
		if errPubsub != nil {
			log.Printf(".EventMirroringToFileService->Publish(): Error on publishing event (Topic: %s) to in-mem: %v", topicName, &types.Error{
				Path:    "." + params.CallerFunction + ".EventMirroringToFileService->Publish()",
				Message: errPubsub.Error(),
				Error:   errPubsub,
				Type:    "eventMirroringService-error",
			})

			errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToFileService->Publish(): Error on publishing event (Topic: %s) to in-mem: %v", topicName, &types.Error{
				Path:    "." + params.CallerFunction + ".EventMirroringToFileService->Publish()",
				Message: errPubsub.Error(),
				Error:   errPubsub,
				Type:    "eventMirroringService-error",
			}))
			if errNotification != nil {
				log.Println(errNotification)
			}

			if errFileHandler != nil {
				return &types.Error{
					Path: ".EventMirroringToFileService->Publish()",
					Message: fmt.Sprintf(`
						Error on publishing event:
						errFileHandler: %v
						errPubsub: %v
						`,
						errFileHandler.Error(),
						errPubsub.Error(),
					),
					Error: fmt.Errorf(`
						Error on publishing event:
						errFileHandler: %v
						errPubsub: %v
					`, errFileHandler.Error(),
						errPubsub.Error(),
					),
					Type: "eventMirroringService-error",
				}
			}
		}
	}

	return nil
}

// NewEventMirroringToFileService build new event mirroring to file services object
func NewEventMirroringToFileService(
	fileName string,
	serviceName string,
	pubsubTopics map[string]*pubsub.Topic,
	notifier notif.Notifier,
) *EventMirroringToFileService {
	return &EventMirroringToFileService{
		fileName:     fileName,
		serviceName:  serviceName,
		pubsubTopics: pubsubTopics,
		notifier:     notifier,
	}
}
