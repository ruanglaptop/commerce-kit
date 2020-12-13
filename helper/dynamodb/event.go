package dynamodb

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/payfazz/commerce-kit/helper"
	"github.com/payfazz/commerce-kit/notif"
	"github.com/payfazz/commerce-kit/types"
	"gocloud.dev/pubsub"
)

// EventMirroringToDynamoDBService implementor of event mirroring service which stores to dynamo db
type EventMirroringToDynamoDBService struct {
	serviceName  string
	tableName    string
	dynamoDB     *dynamodb.DynamoDB
	pubsubTopics map[string]*pubsub.Topic
	notifier     notif.Notifier
}

// Consume consume event from dynamodb with return the first event from file
func (s *EventMirroringToDynamoDBService) Consume(ctx *context.Context, topicName string) (*helper.Event, *types.Error) {
	eventResult := helper.Event{}
	filter := expression.Name("serviceName").Equal(expression.Value(s.serviceName))
	projection := expression.NamesList(expression.Name("serviceName"), expression.Name("topicName"), expression.Name("idempotentId"), expression.Name("object"), expression.Name("message"))
	expression, err := expression.NewBuilder().WithFilter(filter).WithProjection(projection).Build()
	if err != nil {
		log.Printf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Got error building expression): %v", topicName, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		})

		errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Got error building expression): %v", topicName, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		}))
		if errNotification != nil {
			log.Println(errNotification)
		}

		return nil, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		}
	}

	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expression.Names(),
		ExpressionAttributeValues: expression.Values(),
		FilterExpression:          expression.Filter(),
		ProjectionExpression:      expression.Projection(),
		TableName:                 aws.String(s.tableName),
	}

	result, err := s.dynamoDB.Scan(params)
	if err != nil {
		log.Printf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Query API call failed): %v", topicName, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		})

		errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Query API call failed): %v", topicName, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		}))
		if errNotification != nil {
			log.Println(errNotification)
		}

		return nil, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		}
	}

	for _, i := range result.Items {
		err = dynamodbattribute.UnmarshalMap(i, &eventResult)
		if err != nil {
			fmt.Println("Got error unmarshalling:")
			log.Printf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Got error unmarshalling): %v", topicName, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Consume()",
				Message: err.Error(),
				Error:   err,
				Type:    "eventMirroringService-error",
			})

			errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Got error unmarshalling): %v", topicName, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Consume()",
				Message: err.Error(),
				Error:   err,
				Type:    "eventMirroringService-error",
			}))
			if errNotification != nil {
				log.Println(errNotification)
			}

			return nil, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Consume()",
				Message: err.Error(),
				Error:   err,
				Type:    "eventMirroringService-error",
			}
		}

		break
	}

	return &eventResult, nil
}

// IsExist check whether the event is exist in file
func (s *EventMirroringToDynamoDBService) IsExist(ctx *context.Context, event *helper.Event) bool {
	filter := expression.Name("serviceName").Equal(expression.Value(s.serviceName))
	projection := expression.NamesList(expression.Name("serviceName"), expression.Name("topicName"), expression.Name("idempotentId"), expression.Name("object"), expression.Name("message"))
	expression, err := expression.NewBuilder().WithFilter(filter).WithProjection(projection).Build()
	if err != nil {
		log.Printf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Got error building expression): %v", event.TopicName, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		})

		errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Got error building expression): %v", event.TopicName, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		}))
		if errNotification != nil {
			log.Println(errNotification)
		}

		return false
	}

	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expression.Names(),
		ExpressionAttributeValues: expression.Values(),
		FilterExpression:          expression.Filter(),
		ProjectionExpression:      expression.Projection(),
		TableName:                 aws.String(s.tableName),
	}

	result, err := s.dynamoDB.Scan(params)
	if err != nil {
		log.Printf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Query API call failed): %v", event.TopicName, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		})

		errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Query API call failed): %v", event.TopicName, &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Consume()",
			Message: err.Error(),
			Error:   err,
			Type:    "eventMirroringService-error",
		}))
		if errNotification != nil {
			log.Println(errNotification)
		}

		return false
	}

	for _, i := range result.Items {
		eventResult := helper.Event{}

		err = dynamodbattribute.UnmarshalMap(i, &eventResult)
		if err != nil {
			fmt.Println("Got error unmarshalling:")
			log.Printf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Got error unmarshalling): %v", event.TopicName, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Consume()",
				Message: err.Error(),
				Error:   err,
				Type:    "eventMirroringService-error",
			})

			errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Consume(): Error on consuming event (Topic: %s) from dynamodb (Got error unmarshalling): %v", event.TopicName, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Consume()",
				Message: err.Error(),
				Error:   err,
				Type:    "eventMirroringService-error",
			}))
			if errNotification != nil {
				log.Println(errNotification)
			}

			return false
		}

		if eventResult.ServiceName == event.ServiceName && eventResult.TopicName == event.TopicName && eventResult.IdempotentID == event.IdempotentID && eventResult.Object == event.Object {
			return true
		}
	}

	return false
}

// Acknowledge acknowledge / remove event from dynamodb by idempotentId
func (s *EventMirroringToDynamoDBService) Acknowledge(ctx *context.Context, event *helper.Event) *types.Error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"serviceName": {
				N: aws.String(s.serviceName),
			},
			"topicName": {
				S: aws.String(event.TopicName),
			},
			"idempotentId": {
				S: aws.String(event.IdempotentID),
			},
			"object": {
				S: aws.String(event.Object),
			},
		},
		TableName: aws.String(s.tableName),
	}

	_, err := s.dynamoDB.DeleteItem(input)
	if err != nil {
		log.Printf(".EventMirroringToDynamoDBService->Acknowledge (Got error calling DeleteItem): %v", err)
		s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Acknowledge (Got error calling DeleteItem): %v", err))
		return &types.Error{
			Path:    ".EventMirroringToDynamoDBService->Acknowledge()",
			Message: fmt.Sprintf(".EventMirroringToDynamoDBService->Acknowledge(): %v", err),
			Error:   fmt.Errorf(".EventMirroringToDynamoDBService->Acknowledge(): %v", err),
			Type:    "eventMirroringService-error",
		}
	}

	return nil
}

// Publish publish event with mirroring in file
func (s *EventMirroringToDynamoDBService) Publish(ctx *context.Context, topicNames []string, body []byte, metadata map[string]string, callerFunction string) *types.Error {
	metadata["serviceName"] = s.serviceName
	for _, topicName := range topicNames {
		metadata["action"] = topicName

		event := helper.Event{
			ServiceName:  metadata["serviceName"],
			TopicName:    metadata["action"],
			IdempotentID: metadata["idempotentId"],
			Object:       metadata["object"],
			Message:      fmt.Sprintf("%v", body),
		}

		eventInfo, errMarshal := dynamodbattribute.MarshalMap(event)
		if errMarshal != nil {
			log.Printf(".EventMirroringToDynamoDBService->Publish(): Error on publishing event (Topic: %s) to file: %v", topicName, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Publish()",
				Message: errMarshal.Error(),
				Error:   errMarshal,
				Type:    "eventMirroringService-error",
			})

			errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Publish(): Error on publishing event (Topic: %s) to file: %v", topicName, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Publish()",
				Message: errMarshal.Error(),
				Error:   errMarshal,
				Type:    "eventMirroringService-error",
			}))
			if errNotification != nil {
				log.Println(errNotification)
			}

			return &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Publish()",
				Message: errMarshal.Error(),
				Error:   errMarshal,
				Type:    "eventMirroringService-error",
			}
		}

		eventInputed := &dynamodb.PutItemInput{
			Item:      eventInfo,
			TableName: aws.String(s.tableName),
		}

		_, errInputItem := s.dynamoDB.PutItem(eventInputed)
		if errInputItem != nil {
			log.Printf(".EventMirroringToDynamoDBService->Publish(): Error on publishing event (Topic: %s) to file: %v", topicName, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Publish()",
				Message: errInputItem.Error(),
				Error:   errInputItem,
				Type:    "eventMirroringService-error",
			})

			errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Publish(): Error on publishing event (Topic: %s) to file: %v", topicName, &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Publish()",
				Message: errInputItem.Error(),
				Error:   errInputItem,
				Type:    "eventMirroringService-error",
			}))
			if errNotification != nil {
				log.Println(errNotification)
			}

			return &types.Error{
				Path:    ".EventMirroringToDynamoDBService->Publish()",
				Message: errMarshal.Error(),
				Error:   errMarshal,
				Type:    "eventMirroringService-error",
			}
		}

		errPubsub := s.pubsubTopics[topicName].Send(
			*ctx, &pubsub.Message{
				Body:     body,
				Metadata: metadata,
			},
		)
		if errPubsub != nil {
			log.Printf(".EventMirroringToDynamoDBService->Publish(): Error on publishing event (Topic: %s) to in-mem: %v", topicName, &types.Error{
				Path:    "." + callerFunction + ".EventMirroringToDynamoDBService->Publish()",
				Message: errPubsub.Error(),
				Error:   errPubsub,
				Type:    "eventMirroringService-error",
			})

			errNotification := s.notifier.Notify(fmt.Sprintf(".EventMirroringToDynamoDBService->Publish(): Error on publishing event (Topic: %s) to in-mem: %v", topicName, &types.Error{
				Path:    "." + callerFunction + ".EventMirroringToDynamoDBService->Publish()",
				Message: errPubsub.Error(),
				Error:   errPubsub,
				Type:    "eventMirroringService-error",
			}))
			if errNotification != nil {
				log.Println(errNotification)
			}

			return &types.Error{
				Path: ".EventMirroringToDynamoDBService->Publish()",
				Message: fmt.Sprintf(`
						Error on publishing event:
						errDynamoDB: %v - %v
						errPubsub: %v
						`,
					errMarshal.Error(),
					errInputItem.Error(),
					errPubsub.Error(),
				),
				Error: fmt.Errorf(`
						Error on publishing event:
						errDynamoDB: %v - %v
						errPubsub: %v
					`,
					errMarshal.Error(),
					errInputItem.Error(),
					errPubsub.Error(),
				),
				Type: "eventMirroringService-error",
			}
		}
	}

	return nil
}

// NewEventMirroringToDynamoDBService build new event mirroring to dynamodb services object
func NewEventMirroringToDynamoDBService(
	awsRegion string,
	awsURL string,
	tableName string,
	serviceName string,
	pubsubTopics map[string]*pubsub.Topic,
	notifier notif.Notifier,
) *EventMirroringToDynamoDBService {
	config := &aws.Config{
		Region:   aws.String(awsRegion),
		Endpoint: aws.String(awsURL),
	}
	sess := session.Must(session.NewSession(config))
	dynamoDB := dynamodb.New(sess)

	return &EventMirroringToDynamoDBService{
		serviceName:  serviceName,
		tableName:    tableName,
		dynamoDB:     dynamoDB,
		pubsubTopics: pubsubTopics,
		notifier:     notifier,
	}
}
