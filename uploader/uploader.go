package uploader

import (
	"context"

	"github.com/payfazz/commerce-kit/types"
)

// File represents url of uploaded file
// swagger:model
type File struct {
	URL string `json:"url"`
}

// Service represents the interface for servicing uploader object
type ServiceInterface interface {
	Upload(ctx context.Context, fileBytes []byte, fileName string) (*File, *types.Error)
}
