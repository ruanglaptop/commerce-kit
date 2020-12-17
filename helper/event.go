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

// EventKey event key
type EventKey struct {
	TopicName    string `json:"topicName"`
	IdempotentID string `json:"idempotentId"`
}

// PublishEventParams encapsulate parameters in publish event method
type PublishEventParams struct {
	TopicNames     []string
	Body           []byte
	Metadata       map[string]string
	CallerFunction string
}

// EventMirroringServiceInterface represents the mirroring services for storing event data
type EventMirroringServiceInterface interface {
	Consume(ctx *context.Context, topicName string) (*Event, *types.Error)
	IsExist(ctx *context.Context, event *Event) bool
	Publish(ctx *context.Context, params *PublishEventParams) *types.Error
	Acknowledge(ctx *context.Context, event *Event) *types.Error
}
