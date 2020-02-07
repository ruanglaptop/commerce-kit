package appcontext

import (
	"context"

	"github.com/payfazz/commerce-kit/types"
)

type contextKey string

const (
	// KeyURLPath represents the url path key in http server context
	KeyURLPath contextKey = "URLPath"

	// KeyHTTPMethodName represents the method name key in http server context
	KeyHTTPMethodName contextKey = "HTTPMethodName"

	// KeySessionID represents the current logged-in SessionID
	KeySessionID contextKey = "SessionID"

	//KeyWarehouseIDs represents the list access of warehouseId
	KeyWarehouseIDs contextKey = "WarehouseIDs"

	// KeyCurrentAccount represents the CurrentAccountId key in http server context
	KeyCurrentAccount contextKey = "CurrentAccount"

	// KeyOwner represents the OwnerID key in http server context
	KeyOwner contextKey = "Owner"

	// KeyUserID represents the current logged-in UserID
	KeyUserID contextKey = "UserID"

	// KeyLoginToken represents the current logged-in token
	KeyLoginToken contextKey = "LoginToken"

	// KeyCustomerID represents the current logged-in UserID's CustomerID from customer-payfazz
	KeyCustomerID contextKey = "CustomerID"

	// KeyWarehouseID represents the current prefered warehouseID of CustomerID
	KeyWarehouseID contextKey = "WarehouseID"

	// KeyVersionCode represents the current version code of request
	KeyVersionCode contextKey = "VersionCode"

	// KeyCurrentClientAccess represents the Current Client access key in http server context
	KeyCurrentClientAccess contextKey = "CurrentClientAccess"

	// KeyClientID represents the Current Client in http server context
	KeyClientID contextKey = "ClientID"

	// KeyIsSales represents the current type of customer
	KeyIsSales contextKey = "IsSales"

	// KeyWarehouseProvider represents the Current Client in http server context
	KeyWarehouseProvider contextKey = "WarehouseProvider"

	// KeyLogString represents the key Log String in server context
	KeyLogString contextKey = "KeyLogString"

	// KeyAllLog represents the key Log String in server context
	KeyAllLog contextKey = "KeyAllLog"

	// KeyClientRequests represents the all client requests in server context
	KeyClientRequests contextKey = "KeyClientRequests"

	// KeyRequestStatus represents the status of the request
	KeyRequestStatus contextKey = "KeyRequestStatus"

	// KeyRequestHeader represents the header of the request
	KeyRequestHeader contextKey = "KeyRequestHeader"

	// KeyRequestBody represents the body of the request
	KeyRequestBody contextKey = "KeyRequestBody"

	// KeyRequestReferenceID represents the the reference id of a specific request
	KeyRequestReferenceID contextKey = "KeyRequestReferenceID"

	// KeyCurrentXAccessToken represents the current access token of request
	KeyCurrentXAccessToken contextKey = "CurrentAccessToken"

	// KeyCurrentClient represents the Current Client in http server context
	KeyCurrentClient contextKey = "CurrentClient"

	// KeyCurrentClientAndType represents the Current Client in http server context
	KeyCurrentClientAndType contextKey = "CurrentClientAndType"

	// KeyCurrentUserAndType represents the Current User login in http server context
	KeyCurrentUserAndType contextKey = "CurrentUserAndType"

	// KeyCurrentCustomerAndType represents the Current Customer login in http server context
	KeyCurrentCustomerAndType contextKey = "CurrentCustomerAndType"
)

// Owner gets the data owner from the context
func Owner(ctx *context.Context) *int {
	owner := (*ctx).Value(KeyOwner)
	if owner != nil {
		v := owner.(int)
		return &v
	}
	return nil
}

// URLPath gets the data url path from the context
func URLPath(ctx *context.Context) *string {
	urlPath := (*ctx).Value(KeyURLPath)
	if urlPath != nil {
		v := urlPath.(string)
		return &v
	}
	return nil
}

// HTTPMethodName gets the data http method from the context
func HTTPMethodName(ctx *context.Context) *string {
	httpMethodName := (*ctx).Value(KeyHTTPMethodName)
	if httpMethodName != nil {
		v := httpMethodName.(string)
		return &v
	}
	return nil
}

// SessionID gets the data session id from the context
func SessionID(ctx *context.Context) *string {
	sessionID := (*ctx).Value(KeySessionID)
	if sessionID != nil {
		v := sessionID.(string)
		return &v
	}
	return nil
}

// CurrentAccount gets current account from the context
func CurrentAccount(ctx *context.Context) *int {
	currentAccount := (*ctx).Value(KeyCurrentAccount)
	if currentAccount != nil {
		v := currentAccount.(int)
		return &v
	}
	return nil
}

// CurrentClient gets current client from the context
func CurrentClient(ctx *context.Context) *string {
	currentClientAccess := (*ctx).Value(KeyCurrentClient)
	if currentClientAccess != nil {
		v := currentClientAccess.(string)
		return &v
	}
	return nil
}

// UserID gets current userId logged in from the context
func UserID(ctx *context.Context) *int {
	userID := (*ctx).Value(KeyUserID)
	if userID != nil {
		v := userID.(int)
		return &v
	}
	return nil
}

// CustomerID gets current logged-in UserID's CustomerID from customer-payfazz from context
func CustomerID(ctx *context.Context) int {
	customerID := (*ctx).Value(KeyCustomerID)
	if customerID != nil {
		v := customerID.(int)
		return v
	}
	return 0
}

// WarehouseID gets current prefered warehouseID of CustomerID
func WarehouseID(ctx *context.Context) int {
	warehouseID := (*ctx).Value(KeyWarehouseID)
	if warehouseID != nil {
		v := warehouseID.(int)
		return v
	}
	return 0
}

// VersionCode gets current version code of request
func VersionCode(ctx *context.Context) int {
	versionCode := (*ctx).Value(KeyVersionCode)
	if versionCode != nil {
		v := versionCode.(int)
		return v
	}
	return 0
}

// CurrentClientAccess gets current client id from the context
func CurrentClientAccess(ctx *context.Context) []string {
	currentClientAccess := (*ctx).Value(KeyCurrentClientAccess)
	// datas := reflect.ValueOf(currentClientAccess)
	// if datas.Kind() != reflect.Slice {
	// 	return nil
	// }
	// if currentClientAccess != nil || datas.Len() > 0 {
	// 	v := currentClientAccess.([]string)
	// 	return v
	// }
	if currentClientAccess != nil {
		v := currentClientAccess.([]string)
		return v
	}
	return nil
}

// ClientID gets current client from the context
func ClientID(ctx *context.Context) *int {
	currentClientAccess := (*ctx).Value(KeyClientID)
	if currentClientAccess != nil {
		v := currentClientAccess.(int)
		return &v
	}
	return nil
}

// IsSales gets current type of customer
func IsSales(ctx *context.Context) bool {
	isSales := (*ctx).Value(KeyIsSales)
	if isSales != nil {
		v := isSales.(bool)
		return v
	}
	return false
}

// WarehouseProvider gets current client from the context
func WarehouseProvider(ctx *context.Context) *int {
	warehouseProvider := (*ctx).Value(KeyWarehouseProvider)
	if warehouseProvider != nil {
		v := warehouseProvider.(int)
		return &v
	}
	return nil
}

// LogString gets log String from context
func LogString(ctx *context.Context) *string {
	logString := (*ctx).Value(KeyLogString)
	if logString != nil {
		v := logString.(*string)
		return v
	}
	return nil
}

// AllLog gets log String from context
func AllLog(ctx *context.Context) *string {
	logString := (*ctx).Value(KeyAllLog)
	if logString != nil {
		v := logString.(string)
		return &v
	}
	return nil
}

// ClientRequests gets all client requests
func ClientRequests(ctx *context.Context) interface{} {
	clientRequests := (*ctx).Value(KeyClientRequests)
	if clientRequests != nil {
		v := clientRequests.(interface{})
		return v
	}
	return nil
}

// RequestStatus gets request status from context
func RequestStatus(ctx *context.Context) *string {
	requestStatus := (*ctx).Value(KeyRequestStatus)
	if requestStatus != nil {
		v := requestStatus.(string)
		return &v
	}
	return nil
}

// RequestHeader gets client request header
func RequestHeader(ctx *context.Context) string {
	requestHeader := (*ctx).Value(KeyRequestHeader)
	if requestHeader != nil {
		v := requestHeader.(string)
		return v
	}
	return ""
}

// RequestBody gets client request body
func RequestBody(ctx *context.Context) interface{} {
	requestBody := (*ctx).Value(KeyRequestBody)
	if requestBody != nil {
		v := requestBody.(interface{})
		return v
	}
	return nil
}

// WarehouseIDs gets current list warehouse id access
func WarehouseIDs(ctx *context.Context) []*int {
	warehouseIDs := (*ctx).Value(KeyWarehouseIDs)
	if warehouseIDs != nil {
		v := warehouseIDs.([]*int)
		return v
	}
	return nil
}

// RequestReferenceID gets current reference request id
func RequestReferenceID(ctx *context.Context) int {
	requestReferenceID := (*ctx).Value(KeyRequestReferenceID)
	if requestReferenceID != nil {
		v := requestReferenceID.(int)
		return v
	}
	return 0
}

// CurrentXAccessToken gets current x access token code of request
func CurrentXAccessToken(ctx *context.Context) string {
	currentAccessToken := (*ctx).Value(KeyCurrentXAccessToken)
	if currentAccessToken != nil {
		v := currentAccessToken.(string)
		return v
	}
	return ""
}

// CurrentClientAndType gets current client from the context
func CurrentClientAndType(ctx *context.Context) (int, types.TypeContext) {
	currentClient := (*ctx).Value(KeyCurrentClientAndType)
	if currentClient != nil {
		v := currentClient.(int)
		return v, types.CLIENT
	}
	return 0, types.SYSTEM
}

// CurrentUserAndType gets current user from the context
func CurrentUserAndType(ctx *context.Context) (int, types.TypeContext) {
	currentUser := (*ctx).Value(KeyCurrentUserAndType)
	if currentUser != nil {
		v := currentUser.(int)
		return v, types.USER
	}
	return 0, types.SYSTEM
}

// CurrentCustomerAndType gets current user from the context
func CurrentCustomerAndType(ctx *context.Context) (int, types.TypeContext) {
	currentCustomer := (*ctx).Value(KeyCurrentCustomerAndType)
	if currentCustomer != nil {
		v := currentCustomer.(int)
		return v, types.CUSTOMER
	}
	return 0, types.SYSTEM
}
