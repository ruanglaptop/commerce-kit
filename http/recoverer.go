package http

import (
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
)

// recoverer is a middleware that recovers from panics & logs the panic &
// returns a HTTP 500 (Internal Server Error) status if possible.
func (hs *Server) recoverer(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rvr := recover(); rvr != nil {
				fmt.Fprintf(os.Stderr, "Panic: %+v\n", rvr)
				debug.PrintStack()
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}
