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

// MinimumProgress is the progress a client must make.
type MinimumProgress struct {
	// BodyWindow is how long a request body may take to deliver BodyBytes
	// more bytes; each BodyBytes received starts a new window.
	BodyWindow time.Duration
	BodyBytes  int64
	// WriteWindow is how long one write of a response may wait for the
	// client to read.
	WriteWindow time.Duration
}

// DefaultMinimumProgress asks for 64 KiB of body every 10 seconds, about
// 6.5 KB/s, and for each response write to leave within 30 seconds.
func DefaultMinimumProgress() MinimumProgress {
	return MinimumProgress{
		BodyWindow:  10 * time.Second,
		BodyBytes:   64 << 10,
		WriteWindow: 30 * time.Second,
	}
}

// Middleware applies progress to every request: its body must keep the
// minimum rate, and each response write must leave within the write window.
// A zero window sets no deadline in that direction. Once the handler returns,
// it sends whatever the server still buffers under the write deadline.
func Middleware(progress MinimumProgress) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			current := &transfer{
				control:  http.NewResponseController(w),
				progress: progress,
				http1:    req.ProtoMajor == 1,
			}
			if progress.BodyWindow > 0 && progress.BodyBytes > 0 && req.Body != nil &&
				req.Body != http.NoBody {
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
	progress MinimumProgress
	http1    bool
	body     *deadlineBody

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
	now := time.Now()
	switch {
	case b.windowEnd.IsZero():
		b.windowEnd = now.Add(progress.BodyWindow)
	case now.After(b.lastReturn):
		b.windowEnd = b.windowEnd.Add(now.Sub(b.lastReturn))
	}
	current := b.transfer
	current.mutex.Lock()
	current.reading = true
	deadlineSet := current.control.SetReadDeadline(b.windowEnd) == nil
	current.mutex.Unlock()

	n, err := b.body.Read(target)
	b.lastReturn = time.Now()

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
	if b.windowBytes >= progress.BodyBytes {
		b.windowBytes = 0
		b.windowEnd = b.lastReturn.Add(progress.BodyWindow)
	}
	if tooSlow {
		b.tooSlow = fmt.Errorf(
			"%w: under %d bytes in %s: %w",
			ErrBodyTooSlow,
			progress.BodyBytes,
			progress.BodyWindow,
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
	if !current.bodyFinished && current.progress.BodyWindow > 0 {
		deadlineSet = current.control.SetReadDeadline(time.Now().Add(current.progress.BodyWindow)) == nil
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
	now := time.Now()
	writeSet := progress.WriteWindow > 0 &&
		current.control.SetWriteDeadline(now.Add(progress.WriteWindow)) == nil
	readSet := false
	if current.http1 && !current.bodyFinished && !current.reading && !current.closing && progress.BodyWindow > 0 {
		readSet = current.control.SetReadDeadline(now.Add(progress.BodyWindow)) == nil
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
	now := time.Now()
	if progress.WriteWindow > 0 {
		_ = current.control.SetWriteDeadline(now.Add(progress.WriteWindow))
	}
	if !current.bodyFinished && progress.BodyWindow > 0 {
		_ = current.control.SetReadDeadline(now.Add(progress.BodyWindow))
	}
	current.mutex.Unlock()
	_ = current.control.Flush()
}
