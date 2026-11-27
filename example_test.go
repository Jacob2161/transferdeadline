package transferdeadline_test

import (
	"errors"
	"io"
	"net/http"

	"github.com/jacob2161/transferdeadline"
)

func ExampleMiddleware() {
	upload := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, err := io.Copy(io.Discard, req.Body); err != nil {
			if errors.Is(err, transferdeadline.ErrBodyTooSlow) {
				http.Error(w, "the upload arrived too slowly", http.StatusRequestTimeout)
				return
			}
			http.Error(w, "the upload failed", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	server := &http.Server{
		Addr:    "localhost:8080",
		Handler: transferdeadline.Middleware(nil)(upload),
	}
	_ = server // server.ListenAndServe()
}
