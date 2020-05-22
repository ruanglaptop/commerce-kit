package response

import (
	"fmt"
	"net/http"
)

// TEXT writes text http response
func TEXT(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(status)
	fmt.Fprint(w, data)
}
