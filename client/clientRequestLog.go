package client

import (
	"context"

	"github.com/payfazz/commerce-kit/types"
)

// ClientRequest encapsulated object of http client and client request log for acknowledge used
type ClientRequest struct {
	Client  *HTTPClient
	Request *ClientRequestLog
}

// ClientRequestLog object of client request log (log of request to external client)
// swagger:model
type ClientRequestLog struct {
	ID             int            `json:"id" db:"id"`
	ClientID       int            `json:"clientId" db:"clientId"`
	ClientType     string         `json:"clientType" db:"clientType"`
	TransactionID  int            `json:"transactionId" db:"transactionId"`
	Method         string         `json:"method" db:"method"`
	URL            string         `json:"url" db:"url"`
	Header         string         `json:"header" db:"header"`
	Request        types.Metadata `json:"request" db:"request"`
	Status         string         `json:"status" db:"status"`
	HTTPStatusCode int            `json:"httpStatusCode" db:"httpStatusCode"`
	ReferenceID    int            `json:"referenceId" db:"referenceId"`
}

// FindAllClientRequestLogs represents params to get All Client Request Logs
// swagger:model
type FindAllClientRequestLogs struct {
	Search string `json:"search"`
	Page   int    `json:"page"`
	Limit  int    `json:"limit"`
}

// ClientRequestLogStorage represents the interface for manage client request log object
type ClientRequestLogStorage interface {
	FindAll(ctx *context.Context, params *FindAllClientRequestLogs) []*ClientRequestLog
	FindByID(ctx *context.Context, clientRequestLogID int) *ClientRequestLog
	Insert(ctx *context.Context, clientRequestLog *ClientRequestLog) *ClientRequestLog
	Update(ctx *context.Context, clientRequestLog *ClientRequestLog) *ClientRequestLog
	Delete(ctx *context.Context, clientRequestLogID int)
}
