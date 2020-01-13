package response

import (
	"fmt"
	"net/http"
)

// PDF writes pdf http response
func PDF(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/pdf")
	w.WriteHeader(status)
	fmt.Fprint(w, data)
}
