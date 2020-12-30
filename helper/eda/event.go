package eda

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/payfazz/commerce-kit/notif"
	"github.com/payfazz/commerce-kit/types"
	"gocloud.dev/pubsub"
)

// Message represents message object stores in database
type Message struct {
	Topic                    string     `json:"topic" bson:"topic"`
	Message                  []byte     `json:"message" bson:"message"`
	DistributedTransactionID string     `json:"distributedTransactionId" bson:"distributedTransactionId"`
	UID                      string     `json:"uid" bson:"uid"`
	Path                     string     `json:"path" bson:"path"`
	Method                   string     `json:"method" bson:"method"`
	Action                   string     `json:"action" bson:"action"`
	Status                   string     `json:"status" bson:"status"`
	CollectedAt              *time.Time `json:"collectedAt" bson:"collectedAt"`
	FulfilledAt              *time.Time `json:"fulfilledAt" bson:"fulfilledAt"`
	RetrialAttempts          int        `json:"retrialAttempts" bson:"retrialAttempts"`
	Owner                    *int       `json:"owner" bson:"owner"`
	CreatedAt                time.Time  `json:"createdAt" bson:"createdAt"`
	UpdatedAt                time.Time  `json:"updatedAt" bson:"updatedAt"`
	DeletedAt                *time.Time `json:"deletedAt" bson:"deletedAt"`
}

// PublishEventParams encapsulate parameters in publish event method
type PublishEventParams struct {
	TopicName      string
	Action         string
	Body           []byte
	Metadata       map[string]string
	CallerFunction string
}

// EventPublisherServiceInterface represents the event publisher services in commerce-kit to encapsulate process to publishing event to dynamodb
type EventPublisherServiceInterface interface {
	Publish(ctx *context.Context, params *PublishEventParams) *types.Error
}

// EventPublisherService represents implementation of event publisher service interface
type EventPublisherService struct {
	db          *dynamodb.DynamoDB
	tableName   string
	pubsubTopic *pubsub.Topic
	notifier    notif.Notifier
}

// insertEvent insert event to dynamodb
func (s *EventPublisherService) insertEvent(ctx *context.Context, message *Message) (*Message, *types.Error) {
	attributeValues, err := dynamodbattribute.MarshalMap(message)
	if err != nil {
		return nil, &types.Error{
			Path:    ".MessageDynamoDBStorage->Insert()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		}
	}

	itemInput := &dynamodb.PutItemInput{
		Item:      attributeValues,
		TableName: aws.String(s.tableName),
	}

	itemOutput, err := s.db.PutItem(itemInput)
	if err != nil {
		return nil, &types.Error{
			Path:    ".MessageDynamoDBStorage->Insert()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		}
	}

	err = dynamodbattribute.UnmarshalMap(itemOutput.Attributes, message)
	if err != nil {
		return nil, &types.Error{
			Path:    ".MessageDynamoDBStorage->Insert()",
			Message: err.Error(),
			Error:   err,
			Type:    "dynamodb-error",
		}
	}

	return message, nil
}

// Publish publishing event to aws sns and mirroring to dynamodb
func (s *EventPublisherService) Publish(ctx *context.Context, params *PublishEventParams) *types.Error {
	var err *types.Error
	message := &Message{
		Topic:                    params.TopicName,
		Message:                  params.Body,
		DistributedTransactionID: params.Metadata["distributedTransactionId"],
		UID:                      params.Metadata["uid"],
		Action:                   params.Action,
		Status:                   "published",
		RetrialAttempts:          0,
		CreatedAt:                time.Now().UTC(),
		UpdatedAt:                time.Now().UTC(),
	}

	message, err = s.insertEvent(ctx, message)
	if err != nil {
		log.Printf("```.EventPublisherService->Publish(): Error on publishing event (Topic: %s) to dynamodb: %v```", params.TopicName, &types.Error{
			Path:    ".EventPublisherService->Publish()",
			Message: err.Error.Error(),
			Error:   err.Error,
			Type:    "eventPublisherService-error",
		})

		errNotification := s.notifier.Notify(fmt.Sprintf("```.EventPublisherService->Publish(): Error on publishing event (Topic: %s) to dynamodb: %v```", params.TopicName, &types.Error{
			Path:    ".EventPublisherService->Publish()",
			Message: err.Error.Error(),
			Error:   err.Error,
			Type:    "eventPublisherService-error",
		}))
		if errNotification != nil {
			log.Println(errNotification)
		}

		return &types.Error{
			Path:    ".EventPublisherService->Publish()",
			Message: err.Error.Error(),
			Error:   err.Error,
			Type:    "eventPublisherService-error",
		}
	}

	errPubsub := s.pubsubTopic.Send(
		*ctx, &pubsub.Message{
			Body:     params.Body,
			Metadata: params.Metadata,
		},
	)
	if errPubsub != nil {
		log.Printf("```.EventPublisherService->Publish(): Error on publishing event (Topic: %s) to AWS SNS: %v```", params.TopicName, &types.Error{
			Path:    "." + params.CallerFunction + ".EventPublisherService->Publish()",
			Message: errPubsub.Error(),
			Error:   errPubsub,
			Type:    "eventPublisherService-error",
		})

		errNotification := s.notifier.Notify(fmt.Sprintf("```.EventPublisherService->Publish(): Error on publishing event (Topic: %s) to AWS SNS: %v```", params.TopicName, &types.Error{
			Path:    "." + params.CallerFunction + ".EventPublisherService->Publish()",
			Message: errPubsub.Error(),
			Error:   errPubsub,
			Type:    "eventPublisherService-error",
		}))
		if errNotification != nil {
			log.Println(errNotification)
		}

		return &types.Error{
			Path:    "." + params.CallerFunction + ".EventPublisherService->Publish()",
			Message: errPubsub.Error(),
			Error:   errPubsub,
			Type:    "eventPublisherService-error",
		}
	}
	return nil
}

// NewEventPublisherService build new event publisher service to publish to aws sqs and mirroring to dynamodb services object
func NewEventPublisherService(
	awsRegion string,
	awsURL string,
	awsAccessKeyID string,
	awsSecretAccessKey string,
	awsToken string,
	pubsubTopic *pubsub.Topic,
	notifier notif.Notifier,
	tableName string,
) *EventPublisherService {
	// config := &aws.Config{
	// 	Region:   aws.String(awsRegion),
	// 	Endpoint: aws.String(awsURL),
	// }
	// sess := session.Must(session.NewSession(config))

	awsSession := session.New(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, awsToken),
	})
	dynamoDB := dynamodb.New(awsSession)

	return &EventPublisherService{
		db:          dynamoDB,
		tableName:   tableName,
		pubsubTopic: pubsubTopic,
		notifier:    notifier,
	}
}
