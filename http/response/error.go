package response

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/payfazz/commerce-kit/notif"
	"github.com/payfazz/commerce-kit/types"
	validator "gopkg.in/go-playground/validator.v9"

	"github.com/pkg/errors"
)

//FieldError represents error message for each field
//swagger:model
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

//ErrorResponse represents error message
//swagger:model
type ErrorResponse struct {
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Fields  []*FieldError `json:"fields"`
}

// MakeFieldError create field error object
func MakeFieldError(field string, message string) *FieldError {
	return &FieldError{
		Field:   field,
		Message: message,
	}
}

// Error writes error http response
func Error(w http.ResponseWriter, n notif.Notifier, data string, status int, err types.Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	var errorCode string
	switch status {
	case http.StatusUnauthorized:
		errorCode = "Unauthorized"
	case http.StatusNotFound:
		errorCode = "NotFound"
	case http.StatusBadRequest:
		errorCode = "BadRequest"
	case http.StatusUnprocessableEntity:
		errorCode = "ValidationError"
	}

	errorFields := []*FieldError{}

	switch err.Error.(type) {
	case validator.ValidationErrors:
		data = "Bad Request"
		for _, err := range err.Error.(validator.ValidationErrors) {
			e := MakeFieldError(
				err.Field(),
				err.ActualTag())

			errorFields = append(errorFields, e)
		}
	}

	json.NewEncoder(w).Encode(ErrorResponse{
		Code:    errorCode,
		Message: data,
		Fields:  errorFields,
	})

	if err.Error != nil {
		log.Printf("INFO: %v\n", err.Error.Error())
		log.Printf("DETAIL [%s - %s]: %s\n", err.Path, err.Type, err.Message)
		type stackTracer interface {
			StackTrace() errors.StackTrace
		}

		var st errors.StackTrace
		if err, ok := err.Error.(stackTracer); ok {
			st = err.StackTrace()
			fmt.Printf("INFO: %+v\n", st[0])
		}

		if n != nil && status == http.StatusInternalServerError {
			errMessage := fmt.Sprintf("ERROR: %v\n", err)
			if len(st) > 0 {
				errMessage = fmt.Sprintf("\n\nStack Trace: %v\n", st[0])
			}

			if err := n.Notify(fmt.Sprintf("```%s```", errMessage)); err != nil {
				fmt.Println("NOTIFY TO SLACK ERROR: ", err)
			}
		}
	}
}
