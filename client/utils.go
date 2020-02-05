package client

import (
	"context"
	"fmt"
	"net/url"
	"reflect"

	"github.com/payfazz/commerce-kit/appcontext"
)

// ParseQueryParams .
func ParseQueryParams(path string, params interface{}) string {
	baseURL, _ := url.Parse(path)

	filterParams := baseURL.Query()

	rval := reflect.Indirect(reflect.ValueOf(params))
	rtype := rval.Type()

	for i := 0; i < rval.NumField(); i++ {
		tag := rtype.Field(i).Tag.Get("json")
		switch rval.Field(i).Kind() {
		case reflect.Slice:
			val := rval.Field(i)
			if &val != nil {
				for i := 0; i < val.Len(); i++ {
					filterParams.Add(tag, fmt.Sprintf("%v", val.Index(i)))
				}
			}
			break
		default:
			val := rval.Field(i)
			if &val != nil {
				filterParams.Add(tag, fmt.Sprintf("%v", val))
			}
			break
		}
	}

	baseURL.RawQuery = filterParams.Encode()

	return baseURL.String()
}

func getContextVariables(ctx *context.Context) (int, int, *int) {
	return appcontext.UserID(ctx), appcontext.CustomerID(ctx), appcontext.ClientID(ctx)
}

func determineClient(ctx *context.Context) (int, string) {
	userID, customerID, clientID := getContextVariables(ctx)
	var resUserID int
	var resUserType string
	if userID != 0 {
		resUserID = userID
		resUserType = "User"
	} else if customerID != 0 {
		resUserID = customerID
		resUserType = "Customer"
	} else if clientID != nil && *clientID != 0 {
		resUserID = *clientID
		resUserType = "Client"
	}

	return resUserID, resUserType
}
