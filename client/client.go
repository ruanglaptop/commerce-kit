package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/payfazz/commerce-kit/appcontext"
	"github.com/payfazz/commerce-kit/types"
)

// Method represents the enum for http call method
type Method string

// Enum value for http call method
const (
	POST   Method = "POST"
	PUT    Method = "PUT"
	DELETE Method = "DELETE"
	GET    Method = "GET"
	PATCH  Method = "PATCH"
)

// ResponseError represents struct of Authorization Type
type ResponseError struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Fields     types.Metadata `json:"fields"`
	StatusCode int            `json:"error"`
	Error      error          `json:"error"`
}

// AuthorizationTypeStruct represents struct of Authorization Type
type AuthorizationTypeStruct struct {
	HeaderName      string
	HeaderType      string
	HeaderTypeValue string
	Token           string
}

// AuthorizationType represents the enum for http authorization type
type AuthorizationType AuthorizationTypeStruct

// Enum value for http authorization type
var (
	Basic       = AuthorizationType(AuthorizationTypeStruct{HeaderName: "Authorization", HeaderType: "Basic", HeaderTypeValue: "Basic "})
	Bearer      = AuthorizationType(AuthorizationTypeStruct{HeaderName: "Authorization", HeaderType: "Bearer", HeaderTypeValue: "Bearer "})
	AccessToken = AuthorizationType(AuthorizationTypeStruct{HeaderName: "X-Access-Token", HeaderType: "Auth0", HeaderTypeValue: ""})
)

//
// Private constants
//

const apiURL = "https://127.0.0.1:8080"
const defaultHTTPTimeout = 80 * time.Second
const maxNetworkRetriesDelay = 5000 * time.Millisecond
const minNetworkRetriesDelay = 500 * time.Millisecond

//
// Private variables
//

var httpClient = &http.Client{Timeout: defaultHTTPTimeout}

// GenericHTTPClient represents an interface to generalize an object to implement HTTPClient
type GenericHTTPClient interface {
	Do(req *http.Request) (string, *ResponseError)
	CallClient(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	CallClientWithCircuitBreaker(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	CallClientWithoutLog(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	AddAuthentication(ctx *context.Context, authorizationType AuthorizationType)
}

// HTTPClient represents the service http client
type HTTPClient struct {
	clientRequestLogStorage   ClientRequestLogStorage
	acknowledgeRequestService AcknowledgeRequestServiceInterface
	APIURL                    string
	HTTPClient                *http.Client
	MaxNetworkRetries         int
	UseNormalSleep            bool
	AuthorizationTypes        []AuthorizationType
	ClientName                string
}

func (c *HTTPClient) shouldRetry(err error, res *http.Response, retry int) bool {
	if retry >= c.MaxNetworkRetries {
		return false
	}

	if err != nil {
		return true
	}

	return false
}

func (c *HTTPClient) sleepTime(numRetries int) time.Duration {
	if c.UseNormalSleep {
		return 0
	}

	// exponentially backoff by 2^numOfRetries
	delay := minNetworkRetriesDelay + minNetworkRetriesDelay*time.Duration(1<<uint(numRetries))
	if delay > maxNetworkRetriesDelay {
		delay = maxNetworkRetriesDelay
	}

	// generate random jitter to prevent thundering herd problem
	jitter := rand.Int63n(int64(delay / 4))
	delay -= time.Duration(jitter)

	if delay < minNetworkRetriesDelay {
		delay = minNetworkRetriesDelay
	}

	return delay
}

// Do calls the api http request and parse the response into v
func (c *HTTPClient) Do(req *http.Request) (string, *ResponseError) {
	var res *http.Response
	var err error

	for retry := 0; ; {
		res, err = c.HTTPClient.Do(req)

		if !c.shouldRetry(err, res, retry) {
			break
		}

		sleepDuration := c.sleepTime(retry)
		retry++

		time.Sleep(sleepDuration)
	}
	if err != nil {
		return "", &ResponseError{
			Code:    "",
			Message: "",
			Fields:  nil,
			Error:   err,
		}
	}
	defer res.Body.Close()

	resBody, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return "", &ResponseError{
			Code:       string(res.StatusCode),
			Message:    "",
			Fields:     nil,
			StatusCode: res.StatusCode,
			Error:      err,
		}
	}

	errResponse := &ResponseError{
		Code:       string(res.StatusCode),
		Message:    "",
		Fields:     nil,
		StatusCode: res.StatusCode,
		Error:      nil,
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		err = json.Unmarshal([]byte(string(resBody)), errResponse)
		if err != nil {
			errResponse.Error = err
		}
		return "", errResponse
	}

	return string(resBody), errResponse
}

// CallClient do call client
func (c *HTTPClient) CallClient(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError {
	var jsonData []byte
	var err error
	var response string
	var errDo *ResponseError

	if request != nil && request != "" {
		jsonData, err = json.Marshal(request)
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo
		}
	}

	urlPath, err := url.Parse(fmt.Sprintf("%s/%s", c.APIURL, path))
	if err != nil {
		errDo = &ResponseError{
			Error: err,
		}
		return errDo
	}

	req, err := http.NewRequest(string(method), urlPath.String(), bytes.NewBuffer(jsonData))
	if err != nil {
		errDo = &ResponseError{
			Error: err,
		}
		return errDo
	}

	for _, authorizationType := range c.AuthorizationTypes {
		if authorizationType.HeaderTypeValue != "" {
			req.Header.Add(authorizationType.HeaderName, fmt.Sprintf("%s%s", authorizationType.HeaderTypeValue, authorizationType.Token))
		}
	}
	req.Header.Add("Content-Type", "application/json")

	clientID, clientType := determineClient(ctx)
	requestRaw := types.Metadata{}
	if request != nil && request != "" {
		err = json.Unmarshal(jsonData, &requestRaw)
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo
		}
	}

	var clientRequestLog *ClientRequestLog
	var errClientRequestLog *types.Error
	tempCurrentAccount := appcontext.CurrentAccount(ctx)
	if tempCurrentAccount == nil {
		defaultValue := 0
		tempCurrentAccount = &defaultValue
	}
	requestReferenceID := appcontext.RequestReferenceID(ctx)
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
	if method != GET {
		clientRequestLog, errClientRequestLog = c.clientRequestLogStorage.Insert(&backgroundContext, &ClientRequestLog{
			ClientID:       clientID,
			ClientType:     clientType,
			Method:         string(method),
			URL:            urlPath.String(),
			Header:         fmt.Sprintf("%v", req.Header),
			Request:        requestRaw,
			Status:         "calling",
			HTTPStatusCode: 0,
			ReferenceID:    requestReferenceID,
		})
		if errClientRequestLog != nil {
			if errClientRequestLog.Error != nil {
				errDo = &ResponseError{
					Error: errClientRequestLog.Error,
				}
				return errDo
			}
		}
	}

	response, errDo = c.Do(req)
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		if method != GET {
			clientRequestLog.HTTPStatusCode = errDo.StatusCode
			clientRequestLog.Status = "failed"
			clientRequestLog, errClientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)
			if errClientRequestLog != nil {
				if errClientRequestLog.Error != nil {
					errDo = &ResponseError{
						Error: errClientRequestLog.Error,
					}
					return errDo
				}
			}
		}
		return errDo
	}

	type TransactionID struct {
		ID int `json:"id"`
	}
	var transactionID TransactionID
	json.Unmarshal([]byte(response), &transactionID)

	if method != GET {
		clientRequestLog.TransactionID = transactionID.ID
		if errDo != nil {
			clientRequestLog.HTTPStatusCode = errDo.StatusCode
		}
		clientRequestLog.Status = "success"
		clientRequestLog, errClientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)
		if errClientRequestLog != nil {
			if errClientRequestLog.Error != nil {
				errDo = &ResponseError{
					Error: errClientRequestLog.Error,
				}
				return errDo
			}
		}

		requestStatus := appcontext.RequestStatus(ctx)
		if requestStatus == nil && isAcknowledgeNeeded {
			currentClientRequests := []*ClientRequest{}
			temp := appcontext.ClientRequests(ctx)
			if temp != nil {
				currentClientRequests = temp.([]*ClientRequest)
			}
			currentClientRequests = append(currentClientRequests, &ClientRequest{
				Client:  c,
				Request: clientRequestLog,
			})
			*ctx = context.WithValue(*ctx, appcontext.KeyClientRequests, currentClientRequests)
			// ignore when error occurs
			_ = c.acknowledgeRequestService.Create(&backgroundContext, &AcknowledgeRequest{
				RequestID:          clientRequestLog.ID,
				CommitStatus:       "on_progress",
				ReservedHolder:     requestRaw,
				ReservedHolderName: reflect.TypeOf(request).Elem().Name(),
				Message:            "",
			})
		}
	}

	if response != "" && result != nil {
		if errDo.StatusCode < 200 || errDo.StatusCode >= 300 {
			return errDo
		}

		err = json.Unmarshal([]byte(response), result)
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo
		}
	}

	return errDo
}

// CallClientWithCircuitBreaker do call client with circuit breaker (async)
func (c *HTTPClient) CallClientWithCircuitBreaker(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError {
	var jsonData []byte
	var err error
	var response string
	var errDo *ResponseError

	err = hystrix.Do(c.ClientName, func() error {
		if request != nil {
			jsonData, err = json.Marshal(request)
			if err != nil {
				errDo = &ResponseError{
					Error: err,
				}
				return errDo.Error
			}
		}

		urlPath, err := url.Parse(fmt.Sprintf("%s/%s", c.APIURL, path))
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo.Error
		}

		req, err := http.NewRequest(string(method), urlPath.String(), bytes.NewBuffer(jsonData))
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo.Error
		}

		for _, authorizationType := range c.AuthorizationTypes {
			if authorizationType.HeaderTypeValue != "" {
				req.Header.Add(authorizationType.HeaderName, fmt.Sprintf("%s%s", authorizationType.HeaderTypeValue, authorizationType.Token))
			}
		}
		req.Header.Add("Content-Type", "application/json")

		clientID, clientType := determineClient(ctx)
		requestRaw := types.Metadata{}
		if request != nil {
			err = json.Unmarshal(jsonData, &requestRaw)
			if err != nil {
				errDo = &ResponseError{
					Error: err,
				}
				return errDo.Error
			}
		}

		var clientRequestLog *ClientRequestLog
		var errClientRequestLog *types.Error
		tempCurrentAccount := appcontext.CurrentAccount(ctx)
		if tempCurrentAccount == nil {
			defaultValue := 0
			tempCurrentAccount = &defaultValue
		}
		requestReferenceID := appcontext.RequestReferenceID(ctx)
		backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
		if method != GET {
			clientRequestLog, errClientRequestLog = c.clientRequestLogStorage.Insert(&backgroundContext, &ClientRequestLog{
				ClientID:       clientID,
				ClientType:     clientType,
				Method:         string(method),
				URL:            urlPath.String(),
				Header:         fmt.Sprintf("%v", req.Header),
				Request:        requestRaw,
				Status:         "calling",
				HTTPStatusCode: 0,
				ReferenceID:    requestReferenceID,
			})
			if errClientRequestLog != nil {
				if errClientRequestLog.Error != nil {
					errDo = &ResponseError{
						Error: errClientRequestLog.Error,
					}
					return errDo.Error
				}
			}
		}

		response, errDo = c.Do(req)
		if errDo != nil && (errDo.Error != nil || errDo.Message != "") && method != GET {
			clientRequestLog.HTTPStatusCode = errDo.StatusCode
			clientRequestLog.Status = "failed"
			clientRequestLog, errClientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)
			if errClientRequestLog != nil {
				if errClientRequestLog.Error != nil {
					errDo = &ResponseError{
						Error: errClientRequestLog.Error,
					}
					return errDo.Error
				}
			}

			return errDo.Error
		}

		type TransactionID struct {
			ID int `json:"id"`
		}
		var transactionID TransactionID
		json.Unmarshal([]byte(response), &transactionID)

		if method != GET {
			clientRequestLog.TransactionID = transactionID.ID
			if errDo != nil {
				clientRequestLog.HTTPStatusCode = errDo.StatusCode
			}
			clientRequestLog.Status = "success"
			clientRequestLog, errClientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)
			if errClientRequestLog != nil {
				if errClientRequestLog.Error != nil {
					errDo = &ResponseError{
						Error: errClientRequestLog.Error,
					}
					return errDo.Error
				}
			}

			requestStatus := appcontext.RequestStatus(ctx)
			if requestStatus == nil && isAcknowledgeNeeded {
				currentClientRequests := []*ClientRequest{}
				temp := appcontext.ClientRequests(ctx)
				if temp != nil {
					currentClientRequests = temp.([]*ClientRequest)
				}
				currentClientRequests = append(currentClientRequests, &ClientRequest{
					Client:  c,
					Request: clientRequestLog,
				})
				*ctx = context.WithValue(*ctx, appcontext.KeyClientRequests, currentClientRequests)
				// ignore when error occurs
				_ = c.acknowledgeRequestService.Create(&backgroundContext, &AcknowledgeRequest{
					RequestID:          clientRequestLog.ID,
					CommitStatus:       "on_progress",
					ReservedHolder:     requestRaw,
					ReservedHolderName: reflect.TypeOf(request).Elem().Name(),
					Message:            "",
				})
			}
		}

		if response != "" && result != nil {
			if errDo.StatusCode < 200 || errDo.StatusCode >= 300 {
				return errDo
			}

			err = json.Unmarshal([]byte(response), result)
			if err != nil {
				errDo = &ResponseError{
					Error: err,
				}
				return errDo.Error
			}
		}
		return nil
	}, nil)

	return errDo
}

// CallClientWithoutLog do call client without log
func (c *HTTPClient) CallClientWithoutLog(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError {
	var jsonData []byte
	var err error
	var response string
	var errDo *ResponseError

	if request != nil && request != "" {
		jsonData, err = json.Marshal(request)
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo
		}
	}

	urlPath, err := url.Parse(fmt.Sprintf("%s/%s", c.APIURL, path))
	if err != nil {
		errDo = &ResponseError{
			Error: err,
		}
		return errDo
	}

	req, err := http.NewRequest(string(method), urlPath.String(), bytes.NewBuffer(jsonData))
	if err != nil {
		errDo = &ResponseError{
			Error: err,
		}
		return errDo
	}

	for _, authorizationType := range c.AuthorizationTypes {
		if authorizationType.HeaderTypeValue != "" {
			req.Header.Add(authorizationType.HeaderName, fmt.Sprintf("%s%s", authorizationType.HeaderTypeValue, authorizationType.Token))
		}
	}
	req.Header.Add("Content-Type", "application/json")

	response, errDo = c.Do(req)
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		return errDo
	}

	if response != "" && result != nil {
		if errDo.StatusCode < 200 || errDo.StatusCode >= 300 {
			return errDo
		}

		err = json.Unmarshal([]byte(response), result)
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo
		}
	}

	return errDo
}

// AddAuthentication do add authentication
func (c *HTTPClient) AddAuthentication(ctx *context.Context, authorizationType AuthorizationType) {
	c.AuthorizationTypes = append(c.AuthorizationTypes, authorizationType)
}

// NewHTTPClient creates the new http client
func NewHTTPClient(
	config HTTPClient,
	clientRequestLogStorage ClientRequestLogStorage,
	acknowledgeRequestService AcknowledgeRequestServiceInterface,
) *HTTPClient {
	if config.HTTPClient == nil {
		config.HTTPClient = httpClient
	}

	if config.APIURL == "" {
		config.APIURL = apiURL
	}

	return &HTTPClient{
		APIURL:                    config.APIURL,
		HTTPClient:                config.HTTPClient,
		MaxNetworkRetries:         config.MaxNetworkRetries,
		UseNormalSleep:            config.UseNormalSleep,
		AuthorizationTypes:        config.AuthorizationTypes,
		clientRequestLogStorage:   clientRequestLogStorage,
		acknowledgeRequestService: acknowledgeRequestService,
		ClientName:                config.ClientName,
	}
}

// Sethystrix setting for client
func Sethystrix(nameClient string) {
	hystrix.ConfigureCommand(nameClient, hystrix.CommandConfig{
		Timeout:               10000,
		MaxConcurrentRequests: 10,
		ErrorPercentThreshold: 15,
	})
}
