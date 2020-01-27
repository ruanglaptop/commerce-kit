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

// ClientRequestLogPostgresStorage implements the client request log repository service interface
type ClientRequestLogPostgresStorage struct {
	repository data.GenericStorage
}

// FindAll get list of client request logs
func (s *ClientRequestLogPostgresStorage) FindAll(ctx *context.Context, params *client.FindAllClientRequestLogs) []*client.ClientRequestLog {
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

	clientRequestLog := []*client.ClientRequestLog{}
	currentAccount := appcontext.CurrentAccount(ctx)
	var err error

	where := `true`
	if currentAccount != nil {
		where += ` AND "owner" = :currentAccount`
	}
	if params.Search != "" {
		where += ` AND "phone" ILIKE :search`
	}

	where = fmt.Sprintf(`%s ORDER BY "id" DESC`, where)
	if params.Page > 0 && params.Limit > 0 {
		where = fmt.Sprintf(`%s LIMIT :limit OFFSET :offset`, where)
	}

	err = s.repository.Where(ctx, &clientRequestLog, where, map[string]interface{}{
		"currentAccount": currentAccount,
		"limit":          params.Limit,
		"offset":         ((params.Page - 1) * params.Limit),
		"search":         "%" + params.Search + "%",
	})
	if err != nil {
		fmt.Printf(`Error while selecting "clientRequestLog": %v`, types.Error{
			Path:    ".ClientRequestLogPostgresStorage->FindAll()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		})
		return nil
	}

	return clientRequestLog
}

// FindByID get client request log by its id
func (s *ClientRequestLogPostgresStorage) FindByID(ctx *context.Context, clientRequestLogID int) *client.ClientRequestLog {
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

	var clientRequestLog client.ClientRequestLog
	err := s.repository.FindByID(ctx, &clientRequestLog, clientRequestLogID)
	if err != nil {
		fmt.Printf(`Error while collecting "clientRequestLog": %v`, types.Error{
			Path:    ".ClientRequestLogPostgresStorage->FindByID()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		})
		return nil
	}
	return &clientRequestLog
}

// Insert create a new client request log
func (s *ClientRequestLogPostgresStorage) Insert(ctx *context.Context, clientRequestLog *client.ClientRequestLog) *client.ClientRequestLog {
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

	err := s.repository.Insert(ctx, clientRequestLog)
	if err != nil {
		fmt.Printf(`Error while inserting "clientRequestLog": %v`, types.Error{
			Path:    ".ClientRequestLogPostgresStorage->Insert()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		})
		return nil
	}

	return clientRequestLog
}

// Update update a client request log
func (s *ClientRequestLogPostgresStorage) Update(ctx *context.Context, clientRequestLog *client.ClientRequestLog) *client.ClientRequestLog {
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

	err := s.repository.Update(ctx, clientRequestLog)
	if err != nil {
		fmt.Printf(`Error while updating "clientRequestLog": %v`, types.Error{
			Path:    ".ClientRequestLogPostgresStorage->Update()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		})
		return nil
	}

	return clientRequestLog
}

// Delete delete a client request log
func (s *ClientRequestLogPostgresStorage) Delete(ctx *context.Context, clientRequestLogID int) {
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

	err := s.repository.Delete(ctx, clientRequestLogID)
	if err != nil {
		fmt.Printf(`Error while deleting "clientRequestLog": %v`, types.Error{
			Path:    ".ClientRequestLogPostgresStorage->Delete()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		})
	}
}

// NewClientRequestLogPostgresStorage creates new client request log storage
func NewClientRequestLogPostgresStorage(
	repository data.GenericStorage,
) *ClientRequestLogPostgresStorage {
	return &ClientRequestLogPostgresStorage{
		repository: repository,
	}
}
