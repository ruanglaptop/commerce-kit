package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/payfazz/commerce-kit/appcontext"
	"github.com/payfazz/commerce-kit/client"
	"github.com/payfazz/commerce-kit/data"
	"github.com/payfazz/commerce-kit/logperform"
	"github.com/payfazz/commerce-kit/types"
)

// AcknowledgeRequestPostgresStorage implements the acknowledge request repository service interface
type AcknowledgeRequestPostgresStorage struct {
	repository data.GenericStorage
}

// FindAll get list of acknowledge requests
func (s *AcknowledgeRequestPostgresStorage) FindAll(ctx *context.Context, params *client.FindAllAcknowledgeRequests) ([]*client.AcknowledgeRequest, *types.Error) {
	// log start here
	logString := appcontext.LogString(ctx)
	var logMethod = "FindAll"
	var logData = &logperform.LoggerStruct{
		Content:          fmt.Sprintf(contentData, typeData, clientDomain, logMethod),
		CurrentStringLog: logString,
	}
	defer func(t time.Time) {
		since := time.Since(t)
		logData.CalledTime = &since
		logperform.PerformanceLogger(logData)
	}(time.Now().UTC())

	acknowledgeRequest := []*client.AcknowledgeRequest{}
	currentAccount := appcontext.CurrentAccount(ctx)
	var err error

	where := `true`
	if currentAccount != nil {
		where += ` AND "owner" = :currentAccount`
	}

	where = fmt.Sprintf(`%s ORDER BY "id" DESC`, where)
	if params.Page > 0 && params.Limit > 0 {
		where = fmt.Sprintf(`%s LIMIT :limit OFFSET :offset`, where)
	}

	err = s.repository.Where(ctx, &acknowledgeRequest, where, map[string]interface{}{
		"currentAccount": currentAccount,
		"limit":          params.Limit,
		"offset":         ((params.Page - 1) * params.Limit),
		"search":         "%" + params.Search + "%",
	})
	if err != nil {
		return nil, &types.Error{
			Path:    ".AcknowledgeRequestPostgresStorage->FindAll()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return acknowledgeRequest, nil
}

// FindByID get acknowledge request by its id
func (s *AcknowledgeRequestPostgresStorage) FindByID(ctx *context.Context, acknowledgeRequestID int) (*client.AcknowledgeRequest, *types.Error) {
	// log start here
	logString := appcontext.LogString(ctx)
	var logMethod = "FindByID"
	var logData = &logperform.LoggerStruct{
		Content:          fmt.Sprintf(contentData, typeData, clientDomain, logMethod),
		CurrentStringLog: logString,
	}
	defer func(t time.Time) {
		since := time.Since(t)
		logData.CalledTime = &since
		logperform.PerformanceLogger(logData)
	}(time.Now().UTC())

	var acknowledgeRequest client.AcknowledgeRequest
	err := s.repository.FindByID(ctx, &acknowledgeRequest, acknowledgeRequestID)
	if err != nil {
		return nil, &types.Error{
			Path:    ".AcknowledgeRequestPostgresStorage->FindByID()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}
	return &acknowledgeRequest, nil
}

// Insert create a new acknowledge request
func (s *AcknowledgeRequestPostgresStorage) Insert(ctx *context.Context, acknowledgeRequest *client.AcknowledgeRequest) (*client.AcknowledgeRequest, *types.Error) {
	// log start here
	logString := appcontext.LogString(ctx)
	var logMethod = "Insert"
	var logData = &logperform.LoggerStruct{
		Content:          fmt.Sprintf(contentData, typeData, clientDomain, logMethod),
		CurrentStringLog: logString,
	}
	defer func(t time.Time) {
		since := time.Since(t)
		logData.CalledTime = &since
		logperform.PerformanceLogger(logData)
	}(time.Now().UTC())

	err := s.repository.Insert(ctx, acknowledgeRequest)
	if err != nil {
		return nil, &types.Error{
			Path:    ".AcknowledgeRequestPostgresStorage->Insert()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return acknowledgeRequest, nil
}

// Update update an acknowledge request
func (s *AcknowledgeRequestPostgresStorage) Update(ctx *context.Context, acknowledgeRequest *client.AcknowledgeRequest) (*client.AcknowledgeRequest, *types.Error) {
	// log start here
	logString := appcontext.LogString(ctx)
	var logMethod = "Update"
	var logData = &logperform.LoggerStruct{
		Content:          fmt.Sprintf(contentData, typeData, clientDomain, logMethod),
		CurrentStringLog: logString,
	}
	defer func(t time.Time) {
		since := time.Since(t)
		logData.CalledTime = &since
		logperform.PerformanceLogger(logData)
	}(time.Now().UTC())

	err := s.repository.Update(ctx, acknowledgeRequest)
	if err != nil {
		return nil, &types.Error{
			Path:    ".AcknowledgeRequestPostgresStorage->Update()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return acknowledgeRequest, nil
}

// Delete delete an acknowledge request
func (s *AcknowledgeRequestPostgresStorage) Delete(ctx *context.Context, acknowledgeRequestID int) *types.Error {
	// log start here
	logString := appcontext.LogString(ctx)
	var logMethod = "Delete"
	var logData = &logperform.LoggerStruct{
		Content:          fmt.Sprintf(contentData, typeData, clientDomain, logMethod),
		CurrentStringLog: logString,
	}
	defer func(t time.Time) {
		since := time.Since(t)
		logData.CalledTime = &since
		logperform.PerformanceLogger(logData)
	}(time.Now().UTC())

	err := s.repository.Delete(ctx, acknowledgeRequestID)
	if err != nil {
		return &types.Error{
			Path:    ".AcknowledgeRequestPostgresStorage->Delete()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}
	return nil
}

// NewAcknowledgeRequestPostgresStorage creates new acknowledge request storage
func NewAcknowledgeRequestPostgresStorage(
	repository data.GenericStorage,
) *AcknowledgeRequestPostgresStorage {
	return &AcknowledgeRequestPostgresStorage{
		repository: repository,
	}
}
