package client

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/payfazz/commerce-kit/types"
	validator "gopkg.in/go-playground/validator.v9"
)

// ClientCache object of ClientCache to reflect the url-to-cache data for response purpose
// swagger:model
type ClientCache struct {
	ID           int            `json:"-" db:"id"`
	URL          string         `json:"url" db:"url"`
	Method       string         `json:"method" db:"method"`
	ClientID     int            `json:"clientId" db:"clientId"`
	ClientName   string         `json:"clientName" db:"clientName"`
	Response     types.Metadata `json:"response" db:"response"`
	LastAccessed time.Time      `json:"lastAccessed" db:"lastAccessed"`
}

// CreateClientCacheParams represent the http request data for create ClientCache
// swagger:model
type CreateClientCacheParams struct {
	URL          string         `json:"baseUrl"`
	Method       string         `json:"method"`
	ClientID     int            `json:"clientId"`
	ClientName   string         `json:"clientName"`
	Response     types.Metadata `json:"response"`
	LastAccessed time.Time      `json:"lastAccessed"`
}

// UpdateClientCacheParams represent the http request data for update ClientCache
// swagger:model
type UpdateClientCacheParams struct {
	URL          string         `json:"baseUrl"`
	Method       string         `json:"method"`
	ClientID     int            `json:"clientId"`
	ClientName   string         `json:"clientName"`
	Response     types.Metadata `json:"response"`
	LastAccessed time.Time      `json:"lastAccessed"`
}

// GetClientCacheByURLParams params to collect client cache by url
type GetClientCacheByURLParams struct {
	URL      string `json:"url"`
	Method   string `json:"method"`
	IsActive bool   `json:"isActive"`
}

// FindAllClientCachesParams represents params to get ListClientCaches
// swagger:model
type FindAllClientCachesParams struct {
	Search     string `json:"search"`
	Page       int    `json:"page"`
	Limit      int    `json:"limit"`
	Method     string `json:"method"`
	ClientName string `json:"clientName"`
}

// ClientCacheStorage represents the interface for manage ClientCache object
type ClientCacheStorage interface {
	FindByID(ctx *context.Context, clientCacheID int) (*ClientCache, *types.Error)
	FindByURL(ctx *context.Context, url string, method string, bufferedTime *int) (*ClientCache, *types.Error)
	FindAll(ctx *context.Context, params *FindAllClientCachesParams) ([]*ClientCache, *types.Error)
	Insert(ctx *context.Context, clientCache *ClientCache) (*ClientCache, *types.Error)
	Update(ctx *context.Context, clientCache *ClientCache) (*ClientCache, *types.Error)
	Delete(ctx *context.Context, clientCache *ClientCache) *types.Error
}

// ClientCacheServiceInterface represents the interface for servicing ClientCache object
type ClientCacheServiceInterface interface {
	CountClientCache(ctx *context.Context, params *FindAllClientCachesParams) (int, *types.Error)
	GetClientCache(ctx *context.Context, clientCacheID int) (*ClientCache, *types.Error)
	ListClientCaches(ctx *context.Context, params *FindAllClientCachesParams) ([]*ClientCache, *types.Error)
	CreateClientCache(ctx *context.Context, params *CreateClientCacheParams) (*ClientCache, *types.Error)
	UpdateClientCache(ctx *context.Context, clientCache int, params *UpdateClientCacheParams) (*ClientCache, *types.Error)
	DeleteClientCache(ctx *context.Context, clientCacheID int) *types.Error
	IsClientNeedToBeCache(ctx *context.Context, url string, method string) (bool, *types.Error)
	GetClientCacheByURL(ctx *context.Context, params *GetClientCacheByURLParams) (*ClientCache, *types.Error)
}

// ClientCacheService implements the ClientCache repository service interface
type ClientCacheService struct {
	clientCacheRepository ClientCacheStorage
	urlToCache            URLToCacheServiceInterface
}

// IsClientNeedToBeCache a function to validate whether client request needs to be cache
func (s *ClientCacheService) IsClientNeedToBeCache(ctx *context.Context, url string, method string) (bool, *types.Error) {
	urlToCache, err := s.urlToCache.GetURLToCacheByURL(ctx, url, method)
	if err != nil {
		if err.Error.Error() != "data is not found" {
			err.Path = ".ClientCacheService->IsClientNeedToBeCache()" + err.Path
			return false, err
		}
	}

	if urlToCache == nil {
		return false, nil
	}

	if urlToCache.IsBlocked {
		return false, nil
	}

	return true, nil
}

// GetClientCacheByURL get ClientCache by url
func (s *ClientCacheService) GetClientCacheByURL(ctx *context.Context, params *GetClientCacheByURLParams) (*ClientCache, *types.Error) {
	if params.IsActive {
		urlToCache, err := s.urlToCache.GetURLToCacheByURL(ctx, params.URL, params.Method)
		if err != nil {
			err.Path = ".ClientCacheService->GetClientCacheByURL()" + err.Path
			return nil, err
		}

		clientCache, err := s.clientCacheRepository.FindByURL(ctx, params.URL, params.Method, &urlToCache.BufferedTime)
		if err != nil {
			err.Path = ".ClientCacheService->GetClientCacheByURL()" + err.Path
			return nil, err
		}

		return clientCache, nil
	}

	clientCache, err := s.clientCacheRepository.FindByURL(ctx, params.URL, params.Method, nil)
	if err != nil {
		err.Path = ".ClientCacheService->GetClientCacheByURL()" + err.Path
		return nil, err
	}

	return clientCache, nil
}

// GetClientCache get ClientCache by its id
func (s *ClientCacheService) GetClientCache(ctx *context.Context, clientCacheID int) (*ClientCache, *types.Error) {
	clientCache, err := s.clientCacheRepository.FindByID(ctx, clientCacheID)
	if err != nil {
		err.Path = ".ClientCacheService->GetClientCache()" + err.Path
		return nil, err
	}

	return clientCache, nil
}

// CountClientCache get list of ClientCache
func (s *ClientCacheService) CountClientCache(ctx *context.Context, params *FindAllClientCachesParams) (int, *types.Error) {
	params.Limit = 0
	params.Page = 0
	clientCaches, err := s.clientCacheRepository.FindAll(ctx, params)
	if err != nil {
		err.Path = ".ClientCacheService->CountClientCache()" + err.Path
		return 0, err
	}

	return len(clientCaches), nil
}

// ListClientCaches get list of ClientCache
func (s *ClientCacheService) ListClientCaches(ctx *context.Context, params *FindAllClientCachesParams) ([]*ClientCache, *types.Error) {
	clientCaches, err := s.clientCacheRepository.FindAll(ctx, params)
	if err != nil {
		err.Path = ".ClientCacheService->ListClientCaches()" + err.Path
		return nil, err
	}

	return clientCaches, nil
}

// CreateClientCache creates a new ClientCache
func (s *ClientCacheService) CreateClientCache(ctx *context.Context, params *CreateClientCacheParams) (*ClientCache, *types.Error) {
	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})

	errValidation := validate.Struct(params)
	if errValidation != nil {
		return nil, &types.Error{
			Path:    ".ClientCacheService->CreateClientCache()",
			Message: errValidation.Error(),
			Error:   errValidation,
			Type:    "validation-error",
		}
	}

	clientCache := &ClientCache{
		URL:          params.URL,
		Method:       params.Method,
		ClientID:     params.ClientID,
		ClientName:   params.ClientName,
		LastAccessed: time.Now().UTC(),
		Response:     params.Response,
	}

	clientCache, err := s.clientCacheRepository.Insert(ctx, clientCache)
	if err != nil {
		err.Path = ".ClientCacheService->CreateClientCache()" + err.Path
		return nil, err
	}

	return clientCache, nil
}

// UpdateClientCache updates a ClientCache
func (s *ClientCacheService) UpdateClientCache(ctx *context.Context, clientCacheID int, params *UpdateClientCacheParams) (*ClientCache, *types.Error) {
	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})

	errValidation := validate.Struct(params)
	if errValidation != nil {
		return nil, &types.Error{
			Path:    ".ClientCacheService->UpdateClientCache()",
			Message: errValidation.Error(),
			Error:   errValidation,
			Type:    "validation-error",
		}
	}

	clientCache, err := s.clientCacheRepository.FindByID(ctx, clientCacheID)
	if err != nil {
		err.Path = ".ClientCacheService->UpdateClientCache()" + err.Path
		return nil, err
	}

	clientCache.URL = params.URL
	clientCache.Method = params.Method
	clientCache.ClientID = params.ClientID
	clientCache.ClientName = params.ClientName
	clientCache.LastAccessed = time.Now().UTC()
	clientCache.Response = params.Response

	clientCache, err = s.clientCacheRepository.Update(ctx, clientCache)
	if err != nil {
		err.Path = ".ClientCacheService->UpdateClientCache()" + err.Path
		return nil, err
	}

	return clientCache, nil
}

// DeleteClientCache delete ClientCache
func (s *ClientCacheService) DeleteClientCache(ctx *context.Context, clientCacheID int) *types.Error {
	clientCache, err := s.clientCacheRepository.FindByID(ctx, clientCacheID)
	if err != nil {
		err.Path = ".ClientCacheService->DeleteClientCache()" + err.Path
		return err
	}

	err = s.clientCacheRepository.Delete(ctx, clientCache)
	if err != nil {
		err.Path = ".ClientCacheService->DeleteClientCache()" + err.Path
		return err
	}

	return nil
}

// NewClientCacheService creates new ClientCache service
func NewClientCacheService(
	clientCacheRepository ClientCacheStorage,
	urlToCache URLToCacheServiceInterface,
) *ClientCacheService {
	return &ClientCacheService{
		clientCacheRepository: clientCacheRepository,
		urlToCache:            urlToCache,
	}
}
