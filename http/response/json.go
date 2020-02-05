package response

import (
	"encoding/json"
	"net/http"
)

// JSON writes json http response
func JSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// ExtendedJSON writes json http response with extended information
func ExtendedJSON(w http.ResponseWriter, code int, data interface{}, metadata map[string]interface{}) {

	var status string
	switch code {
	case http.StatusOK:
		status = "Success"
	case http.StatusCreated:
		status = "Created"
	}

	type response struct {
		Status   string                 `json:"status"`
		Code     int                    `json:"code"`
		Data     interface{}            `json:"data"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}

	resp := response{
		Status:   status,
		Code:     code,
		Data:     data,
		Metadata: metadata,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(resp)
}
