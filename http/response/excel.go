package response

import (
	"fmt"
	"net/http"
	"strings"
)

// EXCEL writes excel http response
func EXCEL(w http.ResponseWriter, status int, data interface{}, filename string) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename="+filename+".xlsx")
	w.Header().Set("Content-Transfer-Encoding", "binary")
	w.Header().Set("Expires", "0")
	writer := strings.NewReader(string([]byte(fmt.Sprintf("%s", data))))
	writer.WriteTo(w)
}
