package response

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/payfazz/commerce-kit/internal/notif"
	"github.com/pkg/errors"
)

type clientResponse struct {
	Status Status      `json:"status"`
	Data   interface{} `json:"data,omitempty"`
}

type Status struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ClientResponse writes client http response
func ClientResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	resp := clientResponse{
		Status: Status{
			Code:    status,
			Message: "success",
		},
		Data: data,
	}

	json.NewEncoder(w).Encode(resp)
}

// ClientError writes client error http response
func ClientError(w http.ResponseWriter, n notif.Notifier, status int, title string, err error) {
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

	json.NewEncoder(w).Encode(clientResponse{
		Status: Status{
			Code:    status,
			Message: errorCode,
		},
		Data: struct {
			Title   string      `json:"title"`
			Content interface{} `json:"content"`
		}{
			Title:   title,
			Content: err.Error(),
		},
	})

	if err != nil {
		log.Printf("INFO: %v\n", err.Error())
		type stackTracer interface {
			StackTrace() errors.StackTrace
		}

		var st errors.StackTrace
		if err, ok := err.(stackTracer); ok {
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
