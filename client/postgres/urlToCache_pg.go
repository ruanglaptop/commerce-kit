package postgres

import (
	"context"
	"fmt"

	"github.com/payfazz/commerce-kit/appcontext"
	"github.com/payfazz/commerce-kit/client"
	"github.com/payfazz/commerce-kit/data"
	"github.com/payfazz/commerce-kit/types"
)

// URLToCachePostgresStorage implements the url to cache repository service interface
type URLToCachePostgresStorage struct {
	repository data.GenericStorage
}

// FindByID get urltocache by its id
func (s *URLToCachePostgresStorage) FindByID(ctx *context.Context, urlToCacheID int) (*client.URLToCache, *types.Error) {
	var urlToCache client.URLToCache
	err := s.repository.FindByID(ctx, &urlToCache, urlToCacheID)
	if err != nil {
		return nil, &types.Error{
			Path:    ".URLToCachePostgresStorage->FindByID()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}
	return &urlToCache, nil
}

// FindByURL get urltocache by url
func (s *URLToCachePostgresStorage) FindByURL(ctx *context.Context, url string, method string) (*client.URLToCache, *types.Error) {
	var urlToCache client.URLToCache
	where := `"deletedAt" IS NULL AND :url ILIKE '%' || "baseUrl" || '%' AND "method" = :method`
	query := fmt.Sprintf(`SELECT 
		"urlToCache"."id",
		"urlToCache"."baseUrl",
		"urlToCache"."method",
		"urlToCache"."clientId",
		"urlToCache"."clientName",
		"urlToCache"."bufferedTime",
		"urlToCache"."isBlocked"
	FROM 
		"urlToCache"
	WHERE 
		%s
	`, where)

	err := s.repository.SelectFirstWithQuery(ctx, &urlToCache, query, map[string]interface{}{
		"url":    url,
		"method": method,
	})
	if err != nil {
		return nil, &types.Error{
			Path:    ".URLToCachePostgresStorage->FindByURL()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}
	return &urlToCache, nil
}

// FindAll get list of urlToCache
func (s *URLToCachePostgresStorage) FindAll(ctx *context.Context, params *client.FindAllURLToCachesParams) ([]*client.URLToCache, *types.Error) {
	urlToCache := []*client.URLToCache{}
	currentAccount := appcontext.CurrentAccount(ctx)
	var err error

	where := `true`
	if currentAccount != nil {
		where += ` AND "owner" = :currentAccount`
	}
	if params.Search != "" {
		where += ` AND "key" ILIKE :search`
	}

	where = fmt.Sprintf(`%s ORDER BY "id" DESC`, where)
	if params.Page > 0 && params.Limit > 0 {
		where = fmt.Sprintf(`%s LIMIT :limit OFFSET :offset`, where)
	}

	err = s.repository.Where(ctx, &urlToCache, where, map[string]interface{}{
		"currentAccount": currentAccount,
		"limit":          params.Limit,
		"offset":         ((params.Page - 1) * params.Limit),
		"search":         "%" + params.Search + "%",
	})
	if err != nil {
		return nil, &types.Error{
			Path:    ".URLToCachePostgresStorage->FindAll()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return urlToCache, nil
}

// Insert create a new urlToCache
func (s *URLToCachePostgresStorage) Insert(ctx *context.Context, urlToCache *client.URLToCache) (*client.URLToCache, *types.Error) {
	err := s.repository.Insert(ctx, urlToCache)
	if err != nil {
		return nil, &types.Error{
			Path:    ".URLToCachePostgresStorage->Insert()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return urlToCache, nil
}

// Update updates a urlToCache
func (s *URLToCachePostgresStorage) Update(ctx *context.Context, urlToCache *client.URLToCache) (*client.URLToCache, *types.Error) {
	err := s.repository.Update(ctx, urlToCache)
	if err != nil {
		return nil, &types.Error{
			Path:    ".URLToCachePostgresStorage->Update()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return urlToCache, nil
}

// Delete delete a urlToCache
func (s *URLToCachePostgresStorage) Delete(ctx *context.Context, urlToCache *client.URLToCache) *types.Error {
	err := s.repository.Delete(ctx, urlToCache.ID)
	if err != nil {
		return &types.Error{
			Path:    ".URLToCachePostgresStorage->Delete()",
			Message: err.Error(),
			Error:   err,
			Type:    "pq-error",
		}
	}

	return nil
}

// NewURLToCachePostgresStorage creates new url to cache app service
func NewURLToCachePostgresStorage(
	repository data.GenericStorage,
) *URLToCachePostgresStorage {
	return &URLToCachePostgresStorage{
		repository: repository,
	}
}
