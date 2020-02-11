package client

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"

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

		case reflect.String:
			val := rval.Field(i).String()
			val = strings.Replace(val, " ", "%20", -1)
			if &val != nil && val != "" {
				filterParams.Add(tag, fmt.Sprintf("%v", val))
			}
			break

		case reflect.Ptr:
			if rval.Field(i).Elem().IsValid() {
				val := rval.Field(i)
				filterParams.Add(tag, fmt.Sprintf("%v", val))
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

func getContextVariables(ctx *context.Context) (*int, int, *int) {
	return appcontext.UserID(ctx), appcontext.CustomerID(ctx), appcontext.ClientID(ctx)
}

func determineClient(ctx *context.Context) (int, string) {
	userID, customerID, clientID := getContextVariables(ctx)
	var resUserID int
	var resUserType string
	if userID != nil {
		resUserID = *userID
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
