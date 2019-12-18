package response

import (
	"fmt"
	"net/http"
)

// HTML writes html http response
func HTML(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(status)
	fmt.Fprint(w, data)
}
