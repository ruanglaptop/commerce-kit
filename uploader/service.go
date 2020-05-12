package uploader

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/nfnt/resize"

	"github.com/payfazz/commerce-kit/appcontext"
	"github.com/payfazz/commerce-kit/logperform"
	"github.com/payfazz/commerce-kit/types"

	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/gcsblob"
	"github.com/google/go-cloud/blob/s3blob"
	"github.com/google/go-cloud/gcp"
)

const (
	uploadDomain = "Upload"
	contentData  = "Call %s - %s - %s"
	typeData     = "Service"
)

// Service represents the uploader service
type Service struct {
	bucket     *blob.Bucket
	bucketName string
	url        string
}

// ConfigParams represents the configuration params for aws
type ConfigParams struct {
	Cloud     string
	Bucket    string
	AccessKey string
	SecretKey string
	Token     string
	URL       string
}

// generateFilePath generate url for new uploaded file
func generateFilePath(currentAccount *int, fileName string) (string, error) {
	utcNow := time.Now().UTC()
	datetimeValue := utcNow.Format("2006_01_02__15_04_05 ")
	buff := make([]byte, 32)
	_, err := rand.Read(buff)
	if err != nil {
		return "", err
	}

	hexString := fmt.Sprintf("%x", buff)
	fileExtension := fileName[strings.LastIndex(fileName, ".")+1 : len(fileName)]
	return fmt.Sprintf("./file/image/%s__%d__%s.%s", datetimeValue, currentAccount, hexString, fileExtension), nil
}

// doDelete process to delete to bucket
func (s *Service) doDelete(ctx *context.Context, url string) *types.Error {

	bucket := "l8ldiytwq83d8ckg"
	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		return nil
	}
	svc := s3.New(sess)

	_, errObject := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(url)})
	if errObject != nil {
		err := &types.Error{
			Path:    ".uploaderService->doDelete()",
			Message: errObject.Error(),
			Error:   errObject,
			Type:    "aws-error",
		}
		return err
	}
	errObject = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(url),
	})
	if errObject != nil {
		err := &types.Error{
			Path:    ".uploaderService->doDelete()",
			Message: errObject.Error(),
			Error:   errObject,
			Type:    "aws-error",
		}
		return err
	}

	return nil
}

// doUpload process to upload to bucket
func (s *Service) doUpload(ctx *context.Context, fileBytes []byte, url string) *types.Error {
	before := func(asFunc func(interface{}) bool) error {
		req := &s3manager.UploadInput{}
		ok := asFunc(&req)
		if !ok {
			return errors.New("invalid s3 type")
		}
		req.ACL = aws.String("public-read")
		return nil
	}
	bw, err := s.bucket.NewWriter(*ctx, url, &blob.WriterOptions{
		BeforeWrite: before,
	})
	if err != nil {
		return &types.Error{
			Path:    ".UploaderService->doUpload()",
			Message: err.Error(),
			Error:   err,
			Type:    "golang-error",
		}
	}

	_, err = bw.Write(fileBytes)
	if err != nil {
		return &types.Error{
			Path:    ".UploaderService->doUpload()",
			Message: err.Error(),
			Error:   err,
			Type:    "golang-error",
		}
	}

	if err = bw.Close(); err != nil {
		return &types.Error{
			Path:    ".UploaderService->doUpload()",
			Message: err.Error(),
			Error:   err,
			Type:    "golang-error",
		}
	}

	return nil
}

func resizeImage(imgBytes []byte) ([]byte, *types.Error) {
	imageInfo, _, errDecodeConfig := image.DecodeConfig(bytes.NewReader(imgBytes))
	if errDecodeConfig != nil {
		return nil, &types.Error{
			Path:    ".UploadController->resizeImage()",
			Message: errDecodeConfig.Error(),
			Error:   errDecodeConfig,
			Type:    "golang-error",
		}
	}

	img, errDecode := jpeg.Decode(bytes.NewReader(imgBytes))
	if errDecode != nil {
		return nil, &types.Error{
			Path:    ".UploadController->resizeImage()",
			Message: errDecode.Error(),
			Error:   errDecode,
			Type:    "golang-error",
		}
	}
	var imgConverted image.Image
	if imageInfo.Width < 1000 {
		imgConverted = resize.Resize(uint((imageInfo.Width * 80 / 100)), 0, img, resize.Lanczos3)
	} else {
		imgConverted = resize.Resize(1000, 0, img, resize.Lanczos3)
	}

	imgBuffer := new(bytes.Buffer)
	errEncode := jpeg.Encode(imgBuffer, imgConverted, nil)
	if errEncode != nil {
		return nil, &types.Error{
			Path:    ".UploadController->resizeImage()",
			Message: errEncode.Error(),
			Error:   errEncode,
			Type:    "golang-error",
		}
	}

	if bytes.NewReader(imgBuffer.Bytes()).Size() > 1000000 { //1MB
		resizeImage(imgBuffer.Bytes())
	}

	return imgBuffer.Bytes(), nil
}

// Upload upload file
func (s *Service) Upload(ctx *context.Context, fileBytes []byte, fileName string) (*File, *types.Error) {
	// log start here
	logString := appcontext.LogString(ctx)
	var logMethod = "Upload"
	var logData = &logperform.LoggerStruct{
		Content:          fmt.Sprintf(contentData, typeData, uploadDomain, logMethod),
		CurrentStringLog: logString,
	}
	defer func(t time.Time) {
		since := time.Since(t)
		logData.CalledTime = &since
		logperform.PerformanceLogger(logData)
	}(time.Now().UTC())

	currentAccount := appcontext.CurrentAccount(ctx)

	url, err := generateFilePath(currentAccount, fileName)
	if err != nil {
		return nil, &types.Error{
			Path:    ".UploaderService->Upload()",
			Message: err.Error(),
			Error:   err,
			Type:    "golang-error",
		}
	}
	var errResize *types.Error
	if http.DetectContentType(fileBytes) != "image/jpeg" || http.DetectContentType(fileBytes) != "image/png" {
		if bytes.NewReader(fileBytes).Size() > 1000000 {
			fileBytes, errResize = resizeImage(fileBytes)
			if errResize != nil {
				errResize.Path = ".UploaderService->Upload()" + errResize.Path
				return nil, errResize
			}
		}
	}
	errUpload := s.doUpload(ctx, fileBytes, url)
	if errUpload != nil {
		errUpload.Path = ".UploaderService->Upload()" + errUpload.Path
		return nil, errUpload
	}

	return &File{
		URL: fmt.Sprintf("%s/%s/%s", s.url, s.bucketName, url[2:len(url)]),
	}, nil
}

// NewService creates new uploader service
func NewService(bucket *blob.Bucket, bucketName string, url string) *Service {
	return &Service{
		bucket:     bucket,
		bucketName: bucketName,
		url:        url,
	}
}

// SetupBucket creates a connection to a particular cloud provider's blob storage.
func SetupBucket(ctx *context.Context, config *ConfigParams) (*blob.Bucket, error) {
	switch config.Cloud {
	case "aws":
		return setupAWS(ctx, config)
	case "gcp":
		return setupGCP(ctx, config.Bucket)
	default:
		return nil, fmt.Errorf("invalid cloud provider: %s", config.Cloud)
	}
}

// setupGCP setupGCP return bucket
func setupGCP(ctx *context.Context, bucket string) (*blob.Bucket, error) {
	// DefaultCredentials assumes a user has logged in with gcloud.
	// See here for more information:
	// https://cloud.google.com/docs/authentication/getting-started
	creds, err := gcp.DefaultCredentials(*ctx)
	if err != nil {
		return nil, err
	}
	c, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, err
	}
	// The bucket name must be globally unique.
	return gcsblob.OpenBucket(*ctx, bucket, c, nil)
}

// setupAWS setupAWS return bucket
func setupAWS(ctx *context.Context, config *ConfigParams) (*blob.Bucket, error) {
	c := &aws.Config{
		// Either hard-code the region or use AWS_REGION.
		Region: aws.String("ap-southeast-1"),
		// credentials.NewEnvCredentials assumes two environment variables are
		// present:
		// 1. AWS_ACCESS_KEY_ID, and
		// 2. AWS_SECRET_ACCESS_KEY.
		// Credentials: credentials.NewEnvCredentials(),
		Credentials: credentials.NewStaticCredentials(
			config.AccessKey,
			config.SecretKey,
			config.Token,
		),
	}
	s := session.Must(session.NewSession(c))
	return s3blob.OpenBucket(*ctx, config.Bucket, s, nil)
}
