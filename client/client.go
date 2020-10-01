package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/go-redis/redis"
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
	// General Error Response
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Fields     types.Metadata `json:"-"`
	StatusCode int            `json:"statusCode"`
	Error      error          `json:"error"`

	// Error Response for Anteraja
	Status int    `json:"status"`
	Info   string `json:"info"`
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
	Secret      = AuthorizationType(AuthorizationTypeStruct{HeaderName: "Secret", HeaderType: "Secret", HeaderTypeValue: ""})
	APIKey      = AuthorizationType(AuthorizationTypeStruct{HeaderName: "APIKey", HeaderType: "APIKey", HeaderTypeValue: ""})
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
	CallClientWithCaching(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	CallClientWithCachingInRedis(ctx *context.Context, durationInSecond int, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	CallClientWithCircuitBreaker(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	CallClientWithoutLog(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	CallClientWithBaseURLGiven(ctx *context.Context, url string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	CallClientWithCustomizedError(ctx *context.Context, path string, method Method, queryParams interface{}, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	CallClientWithCustomizedErrorAndCaching(ctx *context.Context, path string, method Method, queryParams interface{}, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError
	AddAuthentication(ctx *context.Context, authorizationType AuthorizationType)
}

// HTTPClient represents the service http client
type HTTPClient struct {
	clientRequestLogStorage   ClientRequestLogStorage
	acknowledgeRequestService AcknowledgeRequestServiceInterface
	clientCacheService        ClientCacheServiceInterface
	redisClient               *redis.Client
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
		if errResponse.Info != "" {
			errResponse.Message = errResponse.Info
		}
		if errResponse.Status != 0 {
			errResponse.StatusCode = errResponse.Status
		}
		errResponse.Error = fmt.Errorf("Error while calling %s: %v", req.URL.String(), errResponse.Message)

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
		if authorizationType.HeaderType != "APIKey" {
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
	tempCurrentAccount := appcontext.CurrentAccount(ctx)
	if tempCurrentAccount == nil {
		defaultValue := 0
		tempCurrentAccount = &defaultValue
	}
	requestReferenceID := appcontext.RequestReferenceID(ctx)
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
	if method != GET {
		clientRequestLog = c.clientRequestLogStorage.Insert(&backgroundContext, &ClientRequestLog{
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
	}

	response, errDo = c.Do(req)
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		if method != GET {
			clientRequestLog.HTTPStatusCode = errDo.StatusCode
			clientRequestLog.Status = "failed"
			clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)
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
		clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)

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

// CallClientWithCaching do call client if client is unavailable try to collect response from cache when the time is still fulfill
func (c *HTTPClient) CallClientWithCaching(ctx *context.Context, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError {
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

	cachingKey := urlPath.String()
	if method != GET {
		cachingKey = cachingKey + string(jsonData)
	}

	for _, authorizationType := range c.AuthorizationTypes {
		if authorizationType.HeaderType != "APIKey" {
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
	tempCurrentAccount := appcontext.CurrentAccount(ctx)
	if tempCurrentAccount == nil {
		defaultValue := 0
		tempCurrentAccount = &defaultValue
	}
	requestReferenceID := appcontext.RequestReferenceID(ctx)
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
	if method != GET {
		clientRequestLog = c.clientRequestLogStorage.Insert(&backgroundContext, &ClientRequestLog{
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
	}

	isAllowed, errClientCache := c.clientCacheService.IsClientNeedToBeCache(ctx, urlPath.String(), string(method))
	if errClientCache != nil {
		fmt.Printf("\nFailed to IsClientNeedToBeCache while collecting caching information: %v", errClientCache)
	}

	isError := false
	response, errDo = c.Do(req)
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		cachingKey := urlPath.String()
		if method != GET {
			clientRequestLog.HTTPStatusCode = errDo.StatusCode
			clientRequestLog.Status = "failed"
			clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)
		}

		// Do check cache
		isError = true
		if !isAllowed {
			return errDo
		}

		isSuccessCollectingCache := true

		// collect cache
		clientCache, errClientCache := c.clientCacheService.GetClientCacheByURL(ctx, &GetClientCacheByURLParams{
			URL:      cachingKey,
			Method:   string(method),
			IsActive: true,
		})
		if errClientCache != nil {
			fmt.Printf("\nFailed to GetClientCacheByURL while collecting caching: %v", errClientCache)
			fmt.Printf("\n\tParams: %#v", GetClientCacheByURLParams{
				URL:      cachingKey,
				Method:   string(method),
				IsActive: false,
			})
			isSuccessCollectingCache = false
		}

		var bytes []byte
		var errJSON error
		if isSuccessCollectingCache {
			bytes, errJSON = json.Marshal(clientCache.Response)
			if errJSON != nil {
				fmt.Printf("\nFailed to json.Marshal to convert cached response while doing collecting caching data: %v", errJSON)
				isSuccessCollectingCache = false
			}
		}

		if isSuccessCollectingCache {
			fmt.Printf("\n\n============================================================\n")
			fmt.Printf("\nFailed to call client: %#v\n", errDo)
			fmt.Printf("\n\tSuccess collecting last cached and omitting error client\n")
			fmt.Printf("\n============================================================\n\n")
			errDo = nil
		}
		response = string(bytes)
	}

	if isAllowed && !isError {
		// do caching
		isExist := true
		currentClientCache, errClientCache := c.clientCacheService.GetClientCacheByURL(ctx, &GetClientCacheByURLParams{
			URL:      cachingKey,
			Method:   string(method),
			IsActive: false,
		})
		if errClientCache != nil {
			if errClientCache.Message != "data is not found" {
				fmt.Printf("\nFailed to GetClientCacheByURL while collecting caching in order to update cache: %v", errClientCache)
				fmt.Printf("\n\tParams: %#v", GetClientCacheByURLParams{
					URL:      urlPath.String(),
					Method:   string(method),
					IsActive: false,
				})
			}
			isExist = false
		}

		responseInMap := types.Metadata{}
		if response != "" {
			errJSON := json.Unmarshal([]byte(response), &responseInMap)
			if errJSON != nil {
				fmt.Printf("\nFailed to json.Unmarshal to convert response while doing caching: %v", errJSON)
			}
		}

		if isExist {
			// update cache
			currentClientCache.Response = types.Metadata{}
			currentClientCache.Response = responseInMap
			currentClientCache.LastAccessed = time.Now().UTC()

			_, errClientCache = c.clientCacheService.UpdateClientCache(ctx, currentClientCache.ID, &UpdateClientCacheParams{
				URL:          currentClientCache.URL,
				Method:       currentClientCache.Method,
				ClientID:     currentClientCache.ClientID,
				ClientName:   currentClientCache.ClientName,
				Response:     currentClientCache.Response,
				LastAccessed: currentClientCache.LastAccessed,
			})
			if errClientCache != nil {
				fmt.Printf("\nFailed to UpdateClientCache while doing caching: %v", errClientCache)
			}
		} else {
			// create new cache
			tempClientID := appcontext.ClientID(ctx)
			clientID := 0
			if tempClientID != nil {
				clientID = *tempClientID
			}

			_, errClientCache = c.clientCacheService.CreateClientCache(ctx, &CreateClientCacheParams{
				URL:          cachingKey,
				Method:       string(method),
				ClientID:     clientID,
				ClientName:   c.ClientName,
				Response:     responseInMap,
				LastAccessed: time.Now().UTC(),
			})
			if errClientCache != nil {
				fmt.Printf("\nFailed to CreateClientCache while doing caching: %v", errClientCache)
			}
		}
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
		clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)

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

// CallClientWithCachingInRedis call client with caching in redis
func (c *HTTPClient) CallClientWithCachingInRedis(ctx *context.Context, durationInSecond int, path string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError {
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

	//collect from redis if already exist
	val, errRedis := c.redisClient.Get("apicaching:" + urlPath.String()).Result()
	if errRedis != nil {
		log.Printf(`
		======================================================================
		Error Collecting Caching in "CallClientWithCachingInRedis":
		"key": %s
		Error: %v
		======================================================================
		`, "apicaching:"+urlPath.String(), errRedis)
	}

	if val != "" {
		isSuccess := true
		if errJSON := json.Unmarshal([]byte(val), &result); errJSON != nil {
			log.Printf(`
			======================================================================
			Error Collecting Caching in "CallClientWithCachingInRedis":
			"key": %s,
			Error: %v,
			======================================================================
			`, "apicaching:"+urlPath.String(), errJSON)
			isSuccess = false
		}
		if isSuccess {
			return nil
		}
	}

	req, err := http.NewRequest(string(method), urlPath.String(), bytes.NewBuffer(jsonData))
	if err != nil {
		errDo = &ResponseError{
			Error: err,
		}
		return errDo
	}

	for _, authorizationType := range c.AuthorizationTypes {
		if authorizationType.HeaderType != "APIKey" {
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
	tempCurrentAccount := appcontext.CurrentAccount(ctx)
	if tempCurrentAccount == nil {
		defaultValue := 0
		tempCurrentAccount = &defaultValue
	}
	requestReferenceID := appcontext.RequestReferenceID(ctx)
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
	if method != GET {
		clientRequestLog = c.clientRequestLogStorage.Insert(&backgroundContext, &ClientRequestLog{
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
	}

	response, errDo = c.Do(req)
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		if method != GET {
			clientRequestLog.HTTPStatusCode = errDo.StatusCode
			clientRequestLog.Status = "failed"
			clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)
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
		clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)

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
		err = json.Unmarshal([]byte(response), result)
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo
		}

		if errRedis = c.redisClient.Set(
			fmt.Sprintf("%s:%s", "apicaching", urlPath.String()),
			response,
			time.Second*time.Duration(durationInSecond),
		).Err(); err != nil {
			log.Printf(`
			======================================================================
			Error Storing Caching in "CallClientWithCachingInRedis":
			"key": %s,
			Error: %v,
			======================================================================
			`, "apicaching:"+urlPath.String(), err)
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

	Sethystrix(c.ClientName)
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
			if authorizationType.HeaderType != "APIKey" {
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
		tempCurrentAccount := appcontext.CurrentAccount(ctx)
		if tempCurrentAccount == nil {
			defaultValue := 0
			tempCurrentAccount = &defaultValue
		}
		requestReferenceID := appcontext.RequestReferenceID(ctx)
		backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
		if method != GET {
			clientRequestLog = c.clientRequestLogStorage.Insert(&backgroundContext, &ClientRequestLog{
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
		}

		response, errDo = c.Do(req)
		if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
			if method != GET {
				clientRequestLog.HTTPStatusCode = errDo.StatusCode
				clientRequestLog.Status = "failed"
				clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)
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
			clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)

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
		if authorizationType.HeaderType != "APIKey" {
			req.Header.Add(authorizationType.HeaderName, fmt.Sprintf("%s%s", authorizationType.HeaderTypeValue, authorizationType.Token))
		}
	}
	req.Header.Add("Content-Type", "application/json")

	response, errDo = c.Do(req)
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		return errDo
	}

	if response != "" && result != nil {
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

// CallClientWithBaseURLGiven do call client with base url given
func (c *HTTPClient) CallClientWithBaseURLGiven(ctx *context.Context, url string, method Method, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError {
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

	req, err := http.NewRequest(string(method), url, bytes.NewBuffer(jsonData))
	if err != nil {
		errDo = &ResponseError{
			Error: err,
		}
		return errDo
	}

	for _, authorizationType := range c.AuthorizationTypes {
		if authorizationType.HeaderType != "APIKey" {
			req.Header.Add(authorizationType.HeaderName, fmt.Sprintf("%s%s", authorizationType.HeaderTypeValue, authorizationType.Token))
		}
	}
	req.Header.Add("Content-Type", "application/json")

	response, errDo = c.Do(req)
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		return errDo
	}

	if response != "" && result != nil {
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

// CallClientWithCustomizedError do call client with customized error
func (c *HTTPClient) CallClientWithCustomizedError(ctx *context.Context, path string, method Method, queryParams interface{}, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError {
	var jsonData []byte
	var err error
	var errDo *ResponseError

	if request != nil {
		jsonData, err = json.Marshal(request)
		if err != nil {
			errDo = &ResponseError{
				Error: err,
			}
			return errDo
		}
	}

	for _, authorizationType := range c.AuthorizationTypes {
		if authorizationType.HeaderName == "APIKey" {
			s := reflect.ValueOf(queryParams).Elem()
			field := s.FieldByName("APIKey")
			if field.IsValid() {
				field.SetString(authorizationType.Token)
				path = ParseQueryParams(path, queryParams)
			}
		}
	}

	urlPath, err := url.ParseRequestURI(fmt.Sprintf("%s/%s", c.APIURL, path))
	if err != nil {
		errDo = &ResponseError{
			Error: err,
		}
		return errDo
	}

	jsonBuffer := bytes.NewBuffer(jsonData)
	req, err := http.NewRequest(string(method), urlPath.String(), jsonBuffer)
	if err != nil {
		errDo = &ResponseError{
			Error: err,
		}
		return errDo
	}

	for _, authorizationType := range c.AuthorizationTypes {
		if authorizationType.HeaderType != "APIKey" {
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
			return errDo
		}
	}

	var clientRequestLog *ClientRequestLog
	tempCurrentAccount := appcontext.CurrentAccount(ctx)
	if tempCurrentAccount == nil {
		defaultValue := 0
		tempCurrentAccount = &defaultValue
	}

	requestReferenceID := appcontext.RequestReferenceID(ctx)
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
	clientRequestLog = c.clientRequestLogStorage.Insert(&backgroundContext, &ClientRequestLog{
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

	response, errDo := (func() (string, *ResponseError) {
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
			Status:     res.StatusCode,
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			errResponse.Message = string(resBody)
			errResponse.Error = errors.New(string(resBody))

			return "", errResponse
		}

		return string(resBody), errResponse
	})()
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		clientRequestLog.HTTPStatusCode = errDo.StatusCode
		clientRequestLog.Status = "failed"
		clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)

		return errDo
	}

	type TransactionID struct {
		ID int `json:"id"`
	}
	var transactionID TransactionID
	json.Unmarshal([]byte(response), &transactionID)

	if method != GET {
		clientRequestLog.TransactionID = transactionID.ID
	}
	if errDo != nil {
		clientRequestLog.HTTPStatusCode = errDo.Status
	}
	clientRequestLog.Status = "success"
	clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)

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

	if response != "" && result != nil {
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

// CallClientWithCustomizedErrorAndCaching do call client with customized error and caching
func (c *HTTPClient) CallClientWithCustomizedErrorAndCaching(ctx *context.Context, path string, method Method, queryParams interface{}, request interface{}, result interface{}, isAcknowledgeNeeded bool) *ResponseError {
	var jsonData []byte
	var err error
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

	for _, authorizationType := range c.AuthorizationTypes {
		if authorizationType.HeaderName == "APIKey" {
			s := reflect.ValueOf(queryParams).Elem()
			field := s.FieldByName("APIKey")
			if field.IsValid() {
				field.SetString(authorizationType.Token)
				path = ParseQueryParams(path, queryParams)
			}
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
		if authorizationType.HeaderType != "APIKey" {
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
	tempCurrentAccount := appcontext.CurrentAccount(ctx)
	if tempCurrentAccount == nil {
		defaultValue := 0
		tempCurrentAccount = &defaultValue
	}
	requestReferenceID := appcontext.RequestReferenceID(ctx)
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)
	clientRequestLog = c.clientRequestLogStorage.Insert(&backgroundContext, &ClientRequestLog{
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

	isAllowed, errClientCache := c.clientCacheService.IsClientNeedToBeCache(ctx, urlPath.String(), string(method))
	if errClientCache != nil {
		fmt.Printf("\nFailed to IsClientNeedToBeCache while collecting caching information: %v", errClientCache)
	}

	isError := false
	response, errDo := (func() (string, *ResponseError) {
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
			errResponse.Message = string(resBody)
			errResponse.Error = errors.New(string(resBody))
			return "", errResponse
		}

		return string(resBody), errResponse
	})()
	if errDo != nil && (errDo.Error != nil || errDo.Message != "") {
		clientRequestLog.HTTPStatusCode = errDo.StatusCode
		clientRequestLog.Status = "failed"
		clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)

		// Do check cache
		isError = true
		if !isAllowed {
			return errDo
		}

		isSuccessCollectingCache := true

		// collect cache
		clientCache, errClientCache := c.clientCacheService.GetClientCacheByURL(ctx, &GetClientCacheByURLParams{
			URL:      urlPath.String(),
			Method:   string(method),
			IsActive: true,
		})
		if errClientCache != nil {
			fmt.Printf("\nFailed to GetClientCacheByURL while collecting caching: %v", errClientCache)
			fmt.Printf("\n\tParams: %#v", GetClientCacheByURLParams{
				URL:      urlPath.String(),
				Method:   string(method),
				IsActive: false,
			})
			isSuccessCollectingCache = false
		}

		var bytes []byte
		var errJSON error
		if isSuccessCollectingCache {
			bytes, errJSON = json.Marshal(clientCache.Response)
			if errJSON != nil {
				fmt.Printf("\nFailed to json.Marshal to convert cached response while doing collecting caching data: %v", errJSON)
				isSuccessCollectingCache = false
			}
		}

		if isSuccessCollectingCache {
			fmt.Printf("\n\n============================================================\n")
			fmt.Printf("\nFailed to call client: %#v\n", errDo)
			fmt.Printf("\n\tSuccess collecting last cached and omitting error client\n")
			fmt.Printf("\n============================================================\n\n")
			errDo = nil
		}
		response = string(bytes)
	}

	if isAllowed && !isError {
		// do caching
		isExist := true
		currentClientCache, errClientCache := c.clientCacheService.GetClientCacheByURL(ctx, &GetClientCacheByURLParams{
			URL:      urlPath.String(),
			Method:   string(method),
			IsActive: false,
		})
		if errClientCache != nil {
			if errClientCache.Message != "data is not found" {
				fmt.Printf("\nFailed to GetClientCacheByURL while collecting caching in order to update cache: %v", errClientCache)
				fmt.Printf("\n\tParams: %#v", GetClientCacheByURLParams{
					URL:      urlPath.String(),
					Method:   string(method),
					IsActive: false,
				})
			}
			isExist = false
		}

		responseInMap := types.Metadata{}
		if response != "" {
			errJSON := json.Unmarshal([]byte(response), &responseInMap)
			if errJSON != nil {
				fmt.Printf("\nFailed to json.Unmarshal to convert response while doing caching: %v", errJSON)
			}
		}

		if isExist {
			// update cache
			currentClientCache.Response = types.Metadata{}
			currentClientCache.Response = responseInMap
			currentClientCache.LastAccessed = time.Now().UTC()

			_, errClientCache = c.clientCacheService.UpdateClientCache(ctx, currentClientCache.ID, &UpdateClientCacheParams{
				URL:          currentClientCache.URL,
				Method:       currentClientCache.Method,
				ClientID:     currentClientCache.ClientID,
				ClientName:   currentClientCache.ClientName,
				Response:     currentClientCache.Response,
				LastAccessed: currentClientCache.LastAccessed,
			})
			if errClientCache != nil {
				fmt.Printf("\nFailed to UpdateClientCache while doing caching: %v", errClientCache)
			}
		} else {
			// create new cache
			tempClientID := appcontext.ClientID(ctx)
			clientID := 0
			if tempClientID != nil {
				clientID = *tempClientID
			}

			_, errClientCache = c.clientCacheService.CreateClientCache(ctx, &CreateClientCacheParams{
				URL:          urlPath.String(),
				Method:       string(method),
				ClientID:     clientID,
				ClientName:   c.ClientName,
				Response:     responseInMap,
				LastAccessed: time.Now().UTC(),
			})
			if errClientCache != nil {
				fmt.Printf("\nFailed to CreateClientCache while doing caching: %v", errClientCache)
			}
		}
	}

	type TransactionID struct {
		ID int `json:"id"`
	}
	var transactionID TransactionID
	json.Unmarshal([]byte(response), &transactionID)

	if method != GET {
		clientRequestLog.TransactionID = transactionID.ID
	}
	if errDo != nil {
		clientRequestLog.HTTPStatusCode = errDo.StatusCode
	}
	clientRequestLog.Status = "success"
	clientRequestLog = c.clientRequestLogStorage.Update(&backgroundContext, clientRequestLog)

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

	if response != "" && result != nil {
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
	isExist := false
	for key, singleAuthorizationType := range c.AuthorizationTypes {
		if singleAuthorizationType.HeaderType == authorizationType.HeaderType {
			c.AuthorizationTypes[key].Token = authorizationType.Token
			isExist = true
			break
		}
	}

	if isExist == false {
		c.AuthorizationTypes = append(c.AuthorizationTypes, authorizationType)
	}
}

// NewHTTPClient creates the new http client
func NewHTTPClient(
	config HTTPClient,
	clientRequestLogStorage ClientRequestLogStorage,
	acknowledgeRequestService AcknowledgeRequestServiceInterface,
	clientCacheService ClientCacheServiceInterface,
	redisClient *redis.Client,
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
		clientCacheService:        clientCacheService,
		ClientName:                config.ClientName,
		redisClient:               redisClient,
	}
}

// Sethystrix setting for client
func Sethystrix(nameClient string) {
	hystrix.ConfigureCommand(nameClient, hystrix.CommandConfig{
		Timeout:               5000,
		MaxConcurrentRequests: 100,
		ErrorPercentThreshold: 20,
	})
}
