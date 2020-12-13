package helper

import (
	"context"

	"github.com/payfazz/commerce-kit/types"
)

// Event represents event object in for commerces services
type Event struct {
	ServiceName  string `json:"serviceName"`
	TopicName    string `json:"topicName"`
	IdempotentID string `json:"idempotentId"`
	Object       string `json:"object"`
	Message      string `json:"message"`
}

// EventMirroringServiceInterface represents the mirroring services for storing event data
type EventMirroringServiceInterface interface {
	Consume(ctx *context.Context, topicName string) (*Event, *types.Error)
	IsExist(ctx *context.Context, event *Event) bool
	Publish(ctx *context.Context, topicNames []string, body []byte, metadata map[string]string, callerFunction string) *types.Error
	Acknowledge(ctx *context.Context, event *Event) *types.Error
}
