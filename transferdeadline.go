// Package transferdeadline stops clients that stall a transfer: a request
// body that arrives slower than a minimum rate, and a response the client
// stops reading. Deadlines are set through http.ResponseController, so they
// hold per connection over HTTP/1.1 and per stream over HTTP/2 and h2c.
//
// A deadline is armed only while a body read or a response write is
// blocked, and cleared when it returns. Time the server spends between
// reads (waiting for a lock, a database handle, or its own work) is never
// counted against the client, and no deadline is left set to fire while the
// handler works: on HTTP/1.1 a read deadline firing after the body was read
// would cancel the request's context.
//
// When the handler returns, both deadlines are armed once more and left set:
// the server's own last writes (the final chunk and trailers) and its read of
// an unread request body happen after the handler, and are bounded too. The
// server resets the read deadline before the next request on the connection,
// and the next request's first write sets a fresh write deadline. The write
// window replaces http.Server.WriteTimeout for writes through the middleware.
//
// The middleware's response writer supports http.Flusher (and FlushError),
// http.Hijacker, and Unwrap, so http.ResponseController reaches what it wraps.
// Middleware that wraps the http.ResponseWriter outside this one must
// implement Unwrap() http.ResponseWriter too. A framework that renders a
// handler's error after the handler returns, such as Echo, should render it
// inside this middleware, so the error response is written under the write
// deadline too.
package transferdeadline

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// ErrBodyTooSlow reports a request body that arrived slower than the minimum
// rate; a server usually answers it with 408 Request Timeout.
var ErrBodyTooSlow = errors.New("request body arrived too slowly")

const (
	// DefaultBodyWindow is how long a request body may take to deliver
	// DefaultBodyBytes when MiddlewareArgs.BodyWindow is zero.
	DefaultBodyWindow = 10 * time.Second
	// DefaultBodyBytes is the bytes a request body must deliver in each body
	// window, 64 KiB, about 6.5 KB/s at the default window, when
	// MiddlewareArgs.BodyBytes is zero or negative.
	DefaultBodyBytes = 64 << 10
	// DefaultWriteWindow is how long one response write may wait for the
	// client to read when MiddlewareArgs.WriteWindow is zero.
	DefaultWriteWindow = 30 * time.Second
)

// MiddlewareArgs is the progress Middleware asks of every client. Nil and
// the zero value ask for the defaults.
type MiddlewareArgs struct {
	// BodyWindow is how long a request body may take to deliver BodyBytes
	// more bytes; each BodyBytes received starts a new window. Zero uses
	// DefaultBodyWindow; negative sets no read deadline.
	BodyWindow time.Duration
	// BodyBytes is the bytes a request body must deliver in each window.
	// Zero or negative uses DefaultBodyBytes.
	BodyBytes int64
	// WriteWindow is how long one write of a response may wait for the
	// client to read. Zero uses DefaultWriteWindow; negative sets no write
	// deadline.
	WriteWindow time.Duration
}

// minimumProgress is the progress Middleware asks for, with the defaults
// applied; a zero window sets no deadline in that direction.
type minimumProgress struct {
	bodyWindow  time.Duration
	bodyBytes   int64
	writeWindow time.Duration
}

// newMinimumProgress applies the defaults to args, which may be nil.
func newMinimumProgress(args *MiddlewareArgs) minimumProgress {
	if args == nil {
		args = &MiddlewareArgs{}
	}
	progress := minimumProgress{
		bodyWindow:  window(args.BodyWindow, DefaultBodyWindow),
		bodyBytes:   args.BodyBytes,
		writeWindow: window(args.WriteWindow, DefaultWriteWindow),
	}
	if progress.bodyBytes <= 0 {
		progress.bodyBytes = DefaultBodyBytes
	}
	return progress
}

// window is configured, fallback when configured is zero, or zero, meaning
// no deadline, when configured is negative.
func window(configured, fallback time.Duration) time.Duration {
	switch {
	case configured == 0:
		return fallback
	case configured < 0:
		return 0
	}
	return configured
}

// Middleware asks every request for the progress args describe, which may
// be nil for the defaults: its body must keep the minimum rate, and each
// response write must leave within the write window. Once the handler
// returns, it sends whatever the server still buffers under the write
// deadline.
func Middleware(args *MiddlewareArgs) func(http.Handler) http.Handler {
	return middleware(newMinimumProgress(args), time.Now)
}

// middleware is Middleware with the clock it arms deadlines from.
func middleware(progress minimumProgress, now func() time.Time) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			current := &transfer{
				control:  http.NewResponseController(w),
				progress: progress,
				http1:    req.ProtoMajor == 1,
				now:      now,
			}
			if progress.bodyWindow > 0 && req.Body != nil && req.Body != http.NoBody {
				current.body = &deadlineBody{body: req.Body, transfer: current}
				req = req.WithContext(req.Context())
				req.Body = current.body
			} else {
				current.bodyFinished = true
			}
			writer := &deadlineWriter{ResponseWriter: w, transfer: current}
			next.ServeHTTP(writer, req)
			writer.finish()
		})
	}
}

// transfer is one request's deadlines. Its mutex orders the deadline changes
// of a body read and a response write that run at once, as on HTTP/2 and
// full-duplex HTTP/1.1, so neither clears the other's.
type transfer struct {
	control  *http.ResponseController
	progress minimumProgress
	http1    bool
	body     *deadlineBody
	// now is the clock deadlines are armed from: time.Now, or a test's.
	now func() time.Time

	mutex sync.Mutex
	// reading is set while a body read is blocked, writerHoldsRead while a
	// write holds a read deadline, closing while a body Close may read the
	// rest of the body, bodyFinished once the body ended or failed, and
	// hijacked once the handler took the connection. A read deadline is
	// cleared only when none of them needs it.
	reading         bool
	writerHoldsRead bool
	closing         bool
	bodyFinished    bool
	hijacked        bool
}

// deadlineBody reads a request body under the minimum rate. Like any request
// body, it is read by one goroutine at a time.
type deadlineBody struct {
	body     io.ReadCloser
	transfer *transfer
	// windowEnd is when the current window closes, moved later by the time
	// the server spent between reads; windowBytes counts the bytes received
	// in it. lastReturn is when the previous read returned, zero before the
	// first.
	windowEnd   time.Time
	windowBytes int64
	lastReturn  time.Time
	// tooSlow is set once the body missed its window; every read after it
	// fails at once.
	tooSlow error
}

// Read reads with the read deadline armed at the window's end.
func (b *deadlineBody) Read(target []byte) (int, error) {
	if b.tooSlow != nil {
		return 0, b.tooSlow
	}
	progress := b.transfer.progress
	now := b.transfer.now()
	switch {
	case b.windowEnd.IsZero():
		b.windowEnd = now.Add(progress.bodyWindow)
	case now.After(b.lastReturn):
		b.windowEnd = b.windowEnd.Add(now.Sub(b.lastReturn))
	}
	current := b.transfer
	current.mutex.Lock()
	current.reading = true
	deadlineSet := current.control.SetReadDeadline(b.windowEnd) == nil
	current.mutex.Unlock()

	n, err := b.body.Read(target)
	b.lastReturn = current.now()

	current.mutex.Lock()
	current.reading = false
	if err != nil {
		current.bodyFinished = true
	}
	tooSlow := err != nil && errors.Is(err, os.ErrDeadlineExceeded)
	switch {
	case tooSlow:
		// Leave the deadline in the past: HTTP/1.1 reads the rest of an
		// unread body before it answers, to keep the connection, and would
		// wait on the same slow client for as long as it trickles. Failing
		// that read closes the connection after the answer instead.
		_ = current.control.SetReadDeadline(time.Unix(1, 0))
	case deadlineSet && !current.writerHoldsRead && !current.closing:
		_ = current.control.SetReadDeadline(time.Time{})
	}
	current.mutex.Unlock()

	b.windowBytes += int64(n)
	if b.windowBytes >= progress.bodyBytes {
		b.windowBytes = 0
		b.windowEnd = b.lastReturn.Add(progress.bodyWindow)
	}
	if tooSlow {
		b.tooSlow = fmt.Errorf(
			"%w: under %d bytes in %s: %w",
			ErrBodyTooSlow,
			progress.bodyBytes,
			progress.bodyWindow,
			err,
		)
		return n, b.tooSlow
	}
	return n, err
}

// Close closes the body. Closing an unfinished HTTP/1.1 body may read the
// rest of it, so the read is bounded by one body window.
func (b *deadlineBody) Close() error {
	current := b.transfer
	current.mutex.Lock()
	deadlineSet := false
	if !current.bodyFinished && current.progress.bodyWindow > 0 {
		closeDeadline := current.now().Add(current.progress.bodyWindow)
		deadlineSet = current.control.SetReadDeadline(closeDeadline) == nil
		current.closing = deadlineSet
	}
	current.mutex.Unlock()
	err := b.body.Close()
	current.mutex.Lock()
	current.closing = false
	current.bodyFinished = true
	if deadlineSet && !current.reading && !current.writerHoldsRead {
		_ = current.control.SetReadDeadline(time.Time{})
	}
	current.mutex.Unlock()
	return err
}

// deadlineWriter arms the write deadline for each header, write, and flush.
type deadlineWriter struct {
	http.ResponseWriter
	transfer *transfer
}

// setDeadlines sets the write deadline and, on HTTP/1.x while the request
// body is unfinished and not being read, a read deadline one body window
// away: HTTP/1.1 reads the rest of an unread body with its own reader as the
// response starts, which deadlineBody never sees. It returns what clears
// them.
func (w *deadlineWriter) setDeadlines() func() {
	current := w.transfer
	progress := current.progress
	current.mutex.Lock()
	defer current.mutex.Unlock()
	now := current.now()
	writeSet := progress.writeWindow > 0 &&
		current.control.SetWriteDeadline(now.Add(progress.writeWindow)) == nil
	readSet := false
	if current.http1 && !current.bodyFinished && !current.reading && !current.closing &&
		progress.bodyWindow > 0 {
		readSet = current.control.SetReadDeadline(now.Add(progress.bodyWindow)) == nil
		current.writerHoldsRead = readSet
	}
	return func() {
		current.mutex.Lock()
		defer current.mutex.Unlock()
		if writeSet {
			_ = current.control.SetWriteDeadline(time.Time{})
		}
		if readSet {
			current.writerHoldsRead = false
			if !current.reading && !current.closing && !current.bodyFinished {
				_ = current.control.SetReadDeadline(time.Time{})
			}
		}
	}
}

// WriteHeader sends or buffers the header with the deadlines armed: an
// informational (1xx) header is written at once.
func (w *deadlineWriter) WriteHeader(statusCode int) {
	clearDeadlines := w.setDeadlines()
	defer clearDeadlines()
	w.ResponseWriter.WriteHeader(statusCode)
}

// Write writes with the deadlines armed.
func (w *deadlineWriter) Write(content []byte) (int, error) {
	clearDeadlines := w.setDeadlines()
	defer clearDeadlines()
	return w.ResponseWriter.Write(content)
}

// FlushError sends buffered output with the deadlines armed and returns the
// failure to send it.
func (w *deadlineWriter) FlushError() error {
	clearDeadlines := w.setDeadlines()
	defer clearDeadlines()
	return w.transfer.control.Flush()
}

// Flush sends buffered output with the deadlines armed.
func (w *deadlineWriter) Flush() { _ = w.FlushError() }

// Hijack takes over the connection; the middleware then leaves it alone.
func (w *deadlineWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, buffered, err := w.transfer.control.Hijack()
	if err == nil {
		w.transfer.mutex.Lock()
		w.transfer.hijacked = true
		w.transfer.mutex.Unlock()
	}
	return conn, buffered, err
}

// Unwrap returns the wrapped writer for http.ResponseController.
func (w *deadlineWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// finish runs after the handler: it arms the deadlines for the server's own
// last writes and its read of an unread body, leaves them set, and sends what
// is buffered.
func (w *deadlineWriter) finish() {
	current := w.transfer
	progress := current.progress
	current.mutex.Lock()
	if current.hijacked {
		current.mutex.Unlock()
		return
	}
	now := current.now()
	if progress.writeWindow > 0 {
		_ = current.control.SetWriteDeadline(now.Add(progress.writeWindow))
	}
	if !current.bodyFinished && progress.bodyWindow > 0 {
		_ = current.control.SetReadDeadline(now.Add(progress.bodyWindow))
	}
	current.mutex.Unlock()
	_ = current.control.Flush()
}
