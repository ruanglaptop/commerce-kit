package client

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/payfazz/commerce-kit/appcontext"
)

// ParseQueryParams .
func ParseQueryParams(path string, params interface{}) string {
	var listparams []string

	rval := reflect.Indirect(reflect.ValueOf(params))

	rtype := rval.Type()

	for i := 0; i < rval.NumField(); i++ {
		tag := rtype.Field(i).Tag.Get("json")

		switch rval.Field(i).Kind() {
		case reflect.Int:
			val := rval.Field(i).Int()
			if &val != nil {
				listparams = append(listparams, fmt.Sprintf("%s=%v", tag, val))
			}
			break

		case reflect.String:
			val := rval.Field(i).String()
			val = strings.Replace(val, " ", "%20", -1)
			if &val != nil && val != "" {
				listparams = append(listparams, fmt.Sprintf("%s=%v", tag, val))
			}
			break

		case reflect.Slice:
			val := rval.Field(i)
			if &val != nil {
				for i := 0; i < val.Len(); i++ {
					listparams = append(listparams, fmt.Sprintf("%s=%v", tag, val.Index(i)))
				}
			}
			break
		}
	}
	return fmt.Sprintf("%s?%s", path, strings.Join(listparams[:], "&"))
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
