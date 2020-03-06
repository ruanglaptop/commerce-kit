package postgres

import (
	"context"
	"fmt"

	"github.com/payfazz/commerce-kit/appcontext"
	"github.com/payfazz/commerce-kit/client"
	"github.com/payfazz/commerce-kit/data"
	"github.com/payfazz/commerce-kit/types"
)

// ClientCachePostgresStorage implements the client cache repository service interface
type ClientCachePostgresStorage struct {
	repository data.GenericStorage
}

// FindByID get client cache by its id
func (s *ClientCachePostgresStorage) FindByID(ctx *context.Context, clientCacheID int) (*client.ClientCache, *types.Error) {
	var clientCache client.ClientCache
	err := s.repository.FindByID(ctx, &clientCache, clientCacheID)
	if err != nil {
		return nil, &types.Error{
			Path:    ".ClientCachePostgresStorage->FindByID()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}
	return &clientCache, nil
}

// FindAll get list of client cache
func (s *ClientCachePostgresStorage) FindAll(ctx *context.Context, params *client.FindAllClientCachesParams) ([]*client.ClientCache, *types.Error) {
	clientCaches := []*client.ClientCache{}
	currentAccount := appcontext.CurrentAccount(ctx)
	var err error

	where := `true`
	if currentAccount != nil {
		where += ` AND "owner" = :currentAccount`
	}

	if params.Search != "" {
		where += ` AND ("url" ILIKE :search OR "method" ILIKE :search OR "clientName" ILIKE :search)`
	}

	if params.ClientName != "" {
		where += ` AND "clientName" ILIKE :clientName`
	}

	where = fmt.Sprintf(`%s ORDER BY "id" DESC`, where)
	if params.Page > 0 && params.Limit > 0 {
		where = fmt.Sprintf(`%s LIMIT :limit OFFSET :offset`, where)
	}

	err = s.repository.Where(ctx, &clientCaches, where, map[string]interface{}{
		"currentAccount": currentAccount,
		"limit":          params.Limit,
		"offset":         ((params.Page - 1) * params.Limit),
		"search":         "%" + params.Search + "%",
		"clientName":     "%" + params.ClientName + "%",
	})
	if err != nil {
		return nil, &types.Error{
			Path:    ".ClientCachePostgresStorage->FindAll()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return clientCaches, nil
}

// FindByURL get client cache by url
func (s *ClientCachePostgresStorage) FindByURL(ctx *context.Context, url string, method string, bufferedTime *int) (*client.ClientCache, *types.Error) {
	clientCaches := []*client.ClientCache{}
	currentAccount := appcontext.CurrentAccount(ctx)
	var err error

	where := `true`
	if currentAccount != nil {
		where += ` AND "owner" = :currentAccount`
	}

	if url != "" {
		where += ` AND "url" ILIKE :url`
	}

	if method != "" {
		where += ` AND "method" = :method`
	}

	if bufferedTime != nil {
		where += fmt.Sprintf(` AND "lastAccessed" >= NOW() - interval '%d minute'`, *bufferedTime)
	}

	where = fmt.Sprintf(`%s ORDER BY "id" DESC`, where)
	err = s.repository.Where(ctx, &clientCaches, where, map[string]interface{}{
		"currentAccount": currentAccount,
		"url":            "%" + url + "%",
		"method":         method,
	})
	if err != nil {
		return nil, &types.Error{
			Path:    ".CouponPostgresStorage->FindAll()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	if len(clientCaches) == 0 {
		return nil, &types.Error{
			Path:    ".CouponPostgresStorage->FindAll()",
			Message: data.ErrNotFound.Error(),
			Error:   data.ErrNotFound,
			Type:    "pq-error",
		}
	}

	return clientCaches[0], nil
}

// Insert create a new client cache
func (s *ClientCachePostgresStorage) Insert(ctx *context.Context, clientCache *client.ClientCache) (*client.ClientCache, *types.Error) {
	err := s.repository.Insert(ctx, clientCache)
	if err != nil {
		return nil, &types.Error{
			Path:    ".ClientCachePostgresStorage->Insert()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return clientCache, nil
}

// Update updates a client cache
func (s *ClientCachePostgresStorage) Update(ctx *context.Context, clientCache *client.ClientCache) (*client.ClientCache, *types.Error) {
	err := s.repository.Update(ctx, clientCache)
	if err != nil {
		return nil, &types.Error{
			Path:    ".ClientCachePostgresStorage->Update()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return clientCache, nil
}

// Delete delete a coupon
func (s *ClientCachePostgresStorage) Delete(ctx *context.Context, clientCache *client.ClientCache) *types.Error {
	err := s.repository.Delete(ctx, clientCache.ID)
	if err != nil {
		return &types.Error{
			Path:    ".ClientCachePostgresStorage->Delete()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return nil
}

// NewClientCachePostgresStorage creates new client cache repository
func NewClientCachePostgresStorage(
	repository data.GenericStorage,
) *ClientCachePostgresStorage {
	return &ClientCachePostgresStorage{
		repository: repository,
	}
}
