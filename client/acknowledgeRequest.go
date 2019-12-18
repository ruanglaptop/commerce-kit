package client

import (
	"context"

	"github.com/payfazz/commerce-kit/types"
)

// AcknowledgeRequest object of acknowledge request
// swagger:model
type AcknowledgeRequest struct {
	ID                 int            `json:"id" db:"id"`
	RequestID          int            `json:"requestId" db:"requestId"`
	CommitStatus       string         `json:"commitStatus" db:"commitStatus"`
	ReservedHolder     types.Metadata `json:"reservedHolder" db:"reservedHolder"`
	ReservedHolderName string         `json:"reservedHolderName" db:"reservedHolderName"`
	Message            string         `json:"message" db:"message"`
}

// AcknowledgeRequestServiceInterface represents an interface segreggation to encapsulate object of AcknowledgeRequest to control commit
type AcknowledgeRequestServiceInterface interface {
	Acknowledge(ctx context.Context, status string, message string) error
	Create(ctx context.Context, acknowledgeRequest *AcknowledgeRequest) error
}
