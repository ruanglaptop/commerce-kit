package helper

import (
	"context"
	"fmt"
	"log"

	"github.com/payfazz/commerce-kit/notif"
	"github.com/payfazz/commerce-kit/types"

	"gocloud.dev/pubsub"
)

// PublishEventWithMirroringInFile publish event with mirroring in file
func PublishEventWithMirroringInFile(ctx *context.Context, pubsubTopics map[string]*pubsub.Topic, topicNames []string, body []byte, metadata map[string]string, callerFunction string, backupFileName string, notifier notif.Notifier) *types.Error {
	for _, topicName := range topicNames {
		metadata["action"] = topicName
		errPubsub := pubsubTopics[topicName].Send(
			*ctx, &pubsub.Message{
				Body:     body,
				Metadata: metadata,
			},
		)
		if errPubsub != nil {
			log.Printf("[PublishEventWithMirroringInFile] Error on publishing event (Topic: %s) to in-mem: %v", topicName, &types.Error{
				Path:    "." + callerFunction + ".PublishEvent()",
				Message: errPubsub.Error(),
				Error:   errPubsub,
				Type:    "pubsub-error",
			})
			notifier.Notify(fmt.Sprintf("[PublishEventWithMirroringInFile] Error on publishing event (Topic: %s) to in-mem: %v", topicName, &types.Error{
				Path:    "." + callerFunction + ".PublishEvent()",
				Message: errPubsub.Error(),
				Error:   errPubsub,
				Type:    "pubsub-error",
			}))

			log.Println("\nContinue write event to file ...")

			var message string
			for _, value := range metadata {
				message = message + value + "-"
			}
			message = message + fmt.Sprintf("%v", body)

			errFileHandler := AppendToFile(backupFileName, message)
			if errFileHandler != nil {
				return &types.Error{
					Path:    ".PublishEvent()",
					Message: errFileHandler.Error(),
					Error:   errFileHandler,
					Type:    "fileHandler-error",
				}

				notifier.Notify(fmt.Sprintf("[PublishEventWithMirroringInFile] Error on publishing event (Topic: %s) to file: %v", topicName, &types.Error{
					Path:    ".PublishEvent()",
					Message: errFileHandler.Error(),
					Error:   errFileHandler,
					Type:    "fileHandler-error",
				}))
			}
		}
	}

	return nil
}
