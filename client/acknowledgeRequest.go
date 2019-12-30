package client

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/payfazz/commerce-kit/appcontext"
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

// FindAllAcknowledgeRequests represents params to get All acknowledge request
// swagger:model
type FindAllAcknowledgeRequests struct {
	Search string `json:"search"`
	Page   int    `json:"page"`
	Limit  int    `json:"limit"`
}

// AcknowledgeRequestStorage represents the interface for manage acknowledge request object
type AcknowledgeRequestStorage interface {
	FindAll(ctx context.Context, params *FindAllAcknowledgeRequests) ([]*AcknowledgeRequest, *types.Error)
	FindByID(ctx context.Context, acknowledgeRequestID int) (*AcknowledgeRequest, *types.Error)
	Insert(ctx context.Context, acknowledgeRequest *AcknowledgeRequest) (*AcknowledgeRequest, *types.Error)
	Update(ctx context.Context, acknowledgeRequest *AcknowledgeRequest) (*AcknowledgeRequest, *types.Error)
	Delete(ctx context.Context, acknowledgeRequestID int) *types.Error
}

// AcknowledgeRequestServiceInterface represents an interface segreggation to encapsulate object of AcknowledgeRequest to control commit
type AcknowledgeRequestServiceInterface interface {
	Acknowledge(ctx context.Context, status string, message string) error
	Prepare(ctx context.Context) error
	Create(ctx context.Context, acknowledgeRequest *AcknowledgeRequest) error
}

// AcknowledgeRequestService represents the services for acknowledge request
type AcknowledgeRequestService struct {
	clientRequestLog          ClientRequestLogStorage
	acknowledgeRequestStorage AcknowledgeRequestStorage
}

// Create Create log to store the request which needed to be acknowledged
func (s *AcknowledgeRequestService) Create(ctx context.Context, acknowledgeRequest *AcknowledgeRequest) error {
	_, err := s.acknowledgeRequestStorage.Insert(ctx, acknowledgeRequest)
	return err.Error
}

// Prepare store request log to this services before starting run in transaction
func (s *AcknowledgeRequestService) Prepare(ctx *context.Context) error {
	// write request log to this service
	clientID, clientType := determineClient(*ctx)

	methodName := ""
	tMethodName := appcontext.HTTPMethodName(*ctx)
	if tMethodName != nil {
		methodName = *tMethodName
	}

	urlPath := ""
	tURLPath := appcontext.URLPath(*ctx)
	if tURLPath != nil {
		urlPath = *tURLPath
	}

	requestRaw := types.Metadata{}
	if appcontext.RequestBody(*ctx) != nil {
		jsonData, err := json.Marshal(appcontext.RequestBody(*ctx))
		if err != nil {
			return err
		}

		err = json.Unmarshal(jsonData, &requestRaw)
		if err != nil {
			return err
		}
	}

	tempCurrentAccount := appcontext.CurrentAccount(*ctx)
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
	result, errClientRequestLog := s.clientRequestLog.Insert(backgroundContext, &ClientRequestLog{
		ClientID:       clientID,
		ClientType:     clientType,
		Method:         methodName,
		URL:            urlPath,
		Header:         appcontext.RequestHeader(*ctx),
		Request:        requestRaw,
		Status:         "called",
		HTTPStatusCode: 200,
	})
	if errClientRequestLog != nil {
		if errClientRequestLog.Error != nil {
			return errClientRequestLog.Error
		}
	}
	*ctx = context.WithValue(*ctx, appcontext.KeyRequestReferenceID, result.ID)

	return nil
}

// Acknowledge broadcast status (rollback if failed and commit if succeed) request to all request had been sent before
func (s *AcknowledgeRequestService) Acknowledge(ctx context.Context, status string, message string) error {
	ctx = context.WithValue(ctx, appcontext.KeyRequestStatus, &status)
	clientRequests := []*ClientRequest{}
	temp := appcontext.ClientRequests(ctx)
	if temp != nil {
		clientRequests = temp.([]*ClientRequest)
	}

	requestReferenceID := appcontext.RequestReferenceID(ctx)
	currentRequest, err := s.clientRequestLog.FindByID(ctx, requestReferenceID)
	if err != nil {
		return err.Error
	}

	currentRequest.ReferenceID = requestReferenceID
	currentRequest.Status = status
	_, err = s.clientRequestLog.Update(ctx, currentRequest)
	if err != nil {
		return err.Error
	}

	tempCurrentAccount := appcontext.CurrentAccount(ctx)
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
	for _, clientRequest := range clientRequests {
		// acknowledge client to commit / rollback
		var responseResult types.Metadata
		responseError := clientRequest.Client.CallClient(
			ctx,
			fmt.Sprintf("%s?s=%s", clientRequest.Request.URL, status),
			Method(clientRequest.Request.Method),
			clientRequest.Request.Request,
			responseResult,
			false,
		)
		if responseError.Error != nil {
			return responseError.Error
		}

		// ignore when error occurs
		_ = s.Create(backgroundContext, &AcknowledgeRequest{
			RequestID:          clientRequest.Request.ID,
			CommitStatus:       status,
			ReservedHolder:     clientRequest.Request.Request,
			ReservedHolderName: reflect.TypeOf(clientRequest.Request.Request).Elem().Name(),
			Message:            message,
		})
	}

	return nil
}

// NewAcknowledgeRequestService creates new acknowledge service
func NewAcknowledgeRequestService(
	acknowledgeRequestStorage AcknowledgeRequestStorage,
	clientRequestLog ClientRequestLogStorage,
) *AcknowledgeRequestService {
	return &AcknowledgeRequestService{
		clientRequestLog:          clientRequestLog,
		acknowledgeRequestStorage: acknowledgeRequestStorage,
	}
}
