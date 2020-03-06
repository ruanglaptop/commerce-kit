package client

import (
	"context"
	"reflect"
	"strings"

	"github.com/payfazz/commerce-kit/types"
	validator "gopkg.in/go-playground/validator.v9"
)

// URLToCache object of URLToCache to reflect the url-to-cache data for response purpose
// swagger:model
type URLToCache struct {
	ID           int    `json:"-" db:"id"`
	BaseURL      string `json:"baseUrl" db:"baseUrl"`
	Method       string `json:"method" db:"method"`
	ClientID     int    `json:"clientId" db:"clientId"`
	ClientName   string `json:"clientName" db:"clientName"`
	BufferedTime int    `json:"bufferedTime" db:"bufferedTime"`
	IsBlocked    bool   `json:"isBlocked" db:"isBlocked"`
}

// CreateURLToCacheParams represent the http request data for create URLToCache
// swagger:model
type CreateURLToCacheParams struct {
	BaseURL      string `json:"baseUrl"`
	Method       string `json:"method"`
	ClientID     int    `json:"clientId"`
	ClientName   string `json:"clientName"`
	BufferedTime int    `json:"bufferedTime"`
	IsBlocked    bool   `json:"isBlocked"`
}

// UpdateURLToCacheParams represent the http request data for update URLToCache
// swagger:model
type UpdateURLToCacheParams struct {
	BaseURL      string `json:"baseUrl"`
	Method       string `json:"method"`
	ClientID     int    `json:"clientId"`
	ClientName   string `json:"clientName"`
	BufferedTime int    `json:"bufferedTime"`
	IsBlocked    bool   `json:"isBlocked"`
}

// FindAllURLToCachesParams represents params to get ListURLToCaches
// swagger:model
type FindAllURLToCachesParams struct {
	Search       string `json:"search"`
	Page         int    `json:"page"`
	Limit        int    `json:"limit"`
	BufferedTime int    `json:"bufferedTime"`
	IsBlocked    bool   `json:"isBlocked"`
	Method       string `json:"method"`
	ClientName   string `json:"clientName"`
}

// URLToCacheStorage represents the interface for manage URLToCache object
type URLToCacheStorage interface {
	FindByID(ctx *context.Context, urlToCacheID int) (*URLToCache, *types.Error)
	FindByURL(ctx *context.Context, url string, method string) (*URLToCache, *types.Error)
	FindAll(ctx *context.Context, params *FindAllURLToCachesParams) ([]*URLToCache, *types.Error)
	Insert(ctx *context.Context, urlToCache *URLToCache) (*URLToCache, *types.Error)
	Update(ctx *context.Context, urlToCache *URLToCache) (*URLToCache, *types.Error)
	Delete(ctx *context.Context, urlToCache *URLToCache) *types.Error
}

// URLToCacheServiceInterface represents the interface for servicing URLToCache object
type URLToCacheServiceInterface interface {
	CountURLToCache(ctx *context.Context, params *FindAllURLToCachesParams) (int, *types.Error)
	GetURLToCache(ctx *context.Context, couponID int) (*URLToCache, *types.Error)
	GetURLToCacheByURL(ctx *context.Context, url string, method string) (*URLToCache, *types.Error)
	ListURLToCaches(ctx *context.Context, params *FindAllURLToCachesParams) ([]*URLToCache, *types.Error)
	CreateURLToCache(ctx *context.Context, params *CreateURLToCacheParams) (*URLToCache, *types.Error)
	UpdateURLToCache(ctx *context.Context, urlToCache int, params *UpdateURLToCacheParams) (*URLToCache, *types.Error)
	DeleteURLToCache(ctx *context.Context, urlToCacheID int) *types.Error
}

// URLToCacheService implements the URLToCache repository service interface
type URLToCacheService struct {
	urlToCacheRepository URLToCacheStorage
}

// GetURLToCacheByURL get URLToCache by its id
func (s *URLToCacheService) GetURLToCacheByURL(ctx *context.Context, url string, method string) (*URLToCache, *types.Error) {
	urlToCache, err := s.urlToCacheRepository.FindByURL(ctx, url, method)
	if err != nil {
		err.Path = ".URLToCacheService->GetURLToCacheByURL()" + err.Path
		return nil, err
	}

	return urlToCache, nil
}

// GetURLToCache get URLToCache by its id
func (s *URLToCacheService) GetURLToCache(ctx *context.Context, urlToCacheID int) (*URLToCache, *types.Error) {
	urlToCache, err := s.urlToCacheRepository.FindByID(ctx, urlToCacheID)
	if err != nil {
		err.Path = ".URLToCacheService->GetURLToCache()" + err.Path
		return nil, err
	}

	return urlToCache, nil
}

// CountURLToCache get list of URLToCache
func (s *URLToCacheService) CountURLToCache(ctx *context.Context, params *FindAllURLToCachesParams) (int, *types.Error) {
	params.Limit = 0
	params.Page = 0
	urlToCaches, err := s.urlToCacheRepository.FindAll(ctx, params)
	if err != nil {
		err.Path = ".URLToCacheService->CountURLToCache()" + err.Path
		return 0, err
	}

	return len(urlToCaches), nil
}

// ListURLToCaches get list of URLToCache
func (s *URLToCacheService) ListURLToCaches(ctx *context.Context, params *FindAllURLToCachesParams) ([]*URLToCache, *types.Error) {
	urlToCaches, err := s.urlToCacheRepository.FindAll(ctx, params)
	if err != nil {
		err.Path = ".URLToCacheService->ListURLToCaches()" + err.Path
		return nil, err
	}

	return urlToCaches, nil
}

// CreateURLToCache creates a new URLToCache
func (s *URLToCacheService) CreateURLToCache(ctx *context.Context, params *CreateURLToCacheParams) (*URLToCache, *types.Error) {
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
			Path:    ".URLToCacheService->CreateURLToCache()",
			Message: errValidation.Error(),
			Error:   errValidation,
			Type:    "validation-error",
		}
	}

	urlToCache := &URLToCache{
		BaseURL:      params.BaseURL,
		Method:       params.Method,
		ClientID:     params.ClientID,
		ClientName:   params.ClientName,
		BufferedTime: params.BufferedTime,
		IsBlocked:    params.IsBlocked,
	}

	urlToCache, err := s.urlToCacheRepository.Insert(ctx, urlToCache)
	if err != nil {
		err.Path = ".URLToCacheService->CreateURLToCache()" + err.Path
		return nil, err
	}

	return urlToCache, nil
}

//UpdateURLToCache updates a URLToCache
func (s *URLToCacheService) UpdateURLToCache(ctx *context.Context, urlToCacheID int, params *UpdateURLToCacheParams) (*URLToCache, *types.Error) {
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
			Path:    ".URLToCacheService->UpdateURLToCache()",
			Message: errValidation.Error(),
			Error:   errValidation,
			Type:    "validation-error",
		}
	}

	urlToCache, err := s.urlToCacheRepository.FindByID(ctx, urlToCacheID)
	if err != nil {
		err.Path = ".URLToCacheService->UpdateURLToCache()" + err.Path
		return nil, err
	}

	urlToCache.BaseURL = params.BaseURL
	urlToCache.Method = params.Method
	urlToCache.ClientID = params.ClientID
	urlToCache.ClientName = params.ClientName
	urlToCache.BufferedTime = params.BufferedTime
	urlToCache.IsBlocked = params.IsBlocked

	urlToCache, err = s.urlToCacheRepository.Update(ctx, urlToCache)
	if err != nil {
		err.Path = ".URLToCacheService->UpdateURLToCache()" + err.Path
		return nil, err
	}

	return urlToCache, nil
}

// DeleteURLToCache delete URLToCache
func (s *URLToCacheService) DeleteURLToCache(ctx *context.Context, urlToCacheID int) *types.Error {
	urlToCache, err := s.urlToCacheRepository.FindByID(ctx, urlToCacheID)
	if err != nil {
		err.Path = ".URLToCacheService->DeleteURLToCache()" + err.Path
		return err
	}

	err = s.urlToCacheRepository.Delete(ctx, urlToCache)
	if err != nil {
		err.Path = ".URLToCacheService->DeleteURLToCache()" + err.Path
		return err
	}

	return nil
}

// NewURLToCacheService creates new URLToCache service
func NewURLToCacheService(
	urlToCacheRepository URLToCacheStorage,
) *URLToCacheService {
	return &URLToCacheService{
		urlToCacheRepository: urlToCacheRepository,
	}
}
