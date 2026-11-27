# transferdeadline

[![Go Reference](https://pkg.go.dev/badge/github.com/jacob2161/transferdeadline.svg)](https://pkg.go.dev/github.com/jacob2161/transferdeadline)
[![CI](https://github.com/jacob2161/transferdeadline/actions/workflows/ci.yml/badge.svg)](https://github.com/jacob2161/transferdeadline/actions/workflows/ci.yml)

net/http middleware that stops clients stalling a transfer: request bodies that arrive slower than a
minimum rate, and responses the client stops reading. Works over HTTP/1.1, HTTP/2, and h2c.

```bash
go get github.com/jacob2161/transferdeadline
```

## Usage

```go
handler := transferdeadline.Middleware(nil)(mux) // nil asks for the defaults
// In a handler, a body that arrives too slowly fails the read:
if _, err := io.Copy(file, req.Body); errors.Is(err, transferdeadline.ErrBodyTooSlow) {
	http.Error(w, "the upload arrived too slowly", http.StatusRequestTimeout)
}
```

- Unlike `http.Server`'s ReadTimeout and WriteTimeout, which cap a whole request, it asks for
  minimum progress: by default 64 KiB of body every 10 seconds, and each response write leaving
  within 30 seconds. A large, healthy transfer is never cut off.
- Deadlines are armed only while a read or write is blocked, so time the handler spends working is
  never counted against the client.
- After the handler returns, the deadlines stay armed for the server's own final writes and its read
  of an unread body, so a client that stalls then is cut off too.
- Body reads and response writes may run at once (HTTP/2, full duplex) without clearing each other's
  deadlines.
- Its writer supports `http.Flusher` (with `FlushError`), `http.Hijacker`, and `Unwrap`. Middleware
  outside it that wraps the ResponseWriter must implement `Unwrap() http.ResponseWriter` too.

Tested on the two newest Go releases. Licensed under the MIT license.
