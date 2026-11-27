package transferdeadline

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// testProgress asks each response write to leave within 300 ms and 1 KiB of
// body every 300 ms.
var testProgress = &MiddlewareArgs{
	BodyWindow:  300 * time.Millisecond,
	BodyBytes:   1 << 10,
	WriteWindow: 300 * time.Millisecond,
}

// statusRecorder is middleware outside this package's that wraps the
// response writer, as a metrics or logging middleware would, and unwraps it
// for http.ResponseController.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// recordStatus wraps every response writer in a statusRecorder.
func recordStatus(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		next.ServeHTTP(&statusRecorder{ResponseWriter: w}, req)
	})
}

// The protocol tests below run on real time: the deadlines they check are
// socket deadlines the kernel enforces, which testing/synctest's virtual
// clock does not move. TestDeadlinesFollowTheClock checks the deadline
// arithmetic on a clock the test moves.

// serveUnderDeadlines serves route behind the middleware, itself behind a
// middleware that wraps the response writer, over protocol.
func serveUnderDeadlines(
	t *testing.T,
	protocol string,
	route http.HandlerFunc,
) (string, *http.Client) {
	t.Helper()
	server := httptest.NewUnstartedServer(recordStatus(Middleware(testProgress)(route)))
	client := &http.Client{Transport: &http.Transport{}}
	protocols := &http.Protocols{}
	switch protocol {
	case "HTTP/1.1":
		server.Start()
		protocols.SetHTTP1(true)
		client.Transport.(*http.Transport).Protocols = protocols
	case "HTTP/2":
		server.EnableHTTP2 = true
		server.StartTLS()
		client = server.Client()
	case "h2c":
		serverProtocols := &http.Protocols{}
		serverProtocols.SetHTTP1(true)
		serverProtocols.SetUnencryptedHTTP2(true)
		server.Config.Protocols = serverProtocols
		server.Start()
		protocols.SetUnencryptedHTTP2(true)
		client.Transport.(*http.Transport).Protocols = protocols
	}
	t.Cleanup(server.Close)
	t.Cleanup(client.CloseIdleConnections)
	return server.URL, client
}

// TestStoppedReaderEndsTheResponse writes 64 MiB to a client that reads the
// headers and then nothing, over each protocol, and checks that a write
// fails within a few seconds instead of waiting for the client forever.
func TestStoppedReaderEndsTheResponse(t *testing.T) {
	for _, protocol := range []string{"HTTP/1.1", "HTTP/2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			writeFailed := make(chan time.Duration, 1)
			address, client := serveUnderDeadlines(
				t,
				protocol,
				func(w http.ResponseWriter, req *http.Request) {
					started := time.Now()
					chunk := make([]byte, 32<<10)
					w.WriteHeader(http.StatusOK)
					for range 2048 {
						if _, err := w.Write(chunk); err != nil {
							writeFailed <- time.Since(started)
							return
						}
					}
					writeFailed <- -1
				},
			)
			response, err := client.Get(address + "/v1/databases/download")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			select {
			case elapsed := <-writeFailed:
				if elapsed < 0 {
					t.Fatal("64 MiB went to a client that read none of it")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the response still waits on a client that stopped reading")
			}
		})
	}
}

// TestWorkAfterTheBodyPreservesTheRequest reads a JSON body without reaching its
// end, then works for three windows before answering, over each protocol:
// no deadline may be left set to fire meanwhile, which over HTTP/1.1 would
// cancel the request.
func TestWorkAfterTheBodyPreservesTheRequest(t *testing.T) {
	for _, protocol := range []string{"HTTP/1.1", "HTTP/2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			address, client := serveUnderDeadlines(
				t,
				protocol,
				func(w http.ResponseWriter, req *http.Request) {
					var request struct{ Name string }
					if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					select {
					case <-req.Context().Done():
						http.Error(w, "canceled", http.StatusServiceUnavailable)
						return
					case <-time.After(3 * testProgress.BodyWindow):
					}
					_, _ = io.WriteString(w, request.Name)
				},
			)
			response, err := client.Post(
				address+"/v1/locks/acquire",
				"application/json",
				strings.NewReader(`{"name":"kept"}`),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			answer, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != http.StatusOK || string(answer) != "kept" {
				t.Fatalf("the answer after the work: %d %q %v", response.StatusCode, answer, err)
			}
		})
	}
}

// TestUnreadBodyDoesNotHoldTheResponse sends, over HTTP/1.1, a request that
// claims a one-byte body it never sends to a handler that never reads it and
// writes 1 MiB. HTTP/1.1 reads the rest of an unread body as the response
// starts; the handler must still finish within a few body windows.
func TestUnreadBodyDoesNotHoldTheResponse(t *testing.T) {
	finished := make(chan time.Duration, 1)
	address, _ := serveUnderDeadlines(
		t,
		"HTTP/1.1",
		func(w http.ResponseWriter, req *http.Request) {
			started := time.Now()
			chunk := make([]byte, 32<<10)
			w.WriteHeader(http.StatusOK)
			for range 32 {
				if _, err := w.Write(chunk); err != nil {
					break
				}
			}
			finished <- time.Since(started)
		},
	)
	conn, err := net.Dial("tcp", strings.TrimPrefix(address, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(
		conn,
		"GET /v1/databases/download HTTP/1.1\r\nHost: test\r\nContent-Length: 1\r\n\r\n",
	); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, conn) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the response still waits on a request body nobody reads")
	}
}

// TestSlowBodyIsRefused trickles a request body at a byte every 50 ms, far
// below 1 KiB per 300 ms, over each protocol: the handler's read fails with
// ErrBodyTooSlow within a few windows.
func TestSlowBodyIsRefused(t *testing.T) {
	for _, protocol := range []string{"HTTP/1.1", "HTTP/2", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			readFailed := make(chan error, 1)
			address, client := serveUnderDeadlines(
				t,
				protocol,
				func(w http.ResponseWriter, req *http.Request) {
					_, err := io.Copy(io.Discard, req.Body)
					readFailed <- err
					if errors.Is(err, ErrBodyTooSlow) {
						http.Error(w, "too slow", http.StatusRequestTimeout)
					}
				},
			)
			body, trickle := io.Pipe()
			go func() {
				for range 200 {
					if _, err := trickle.Write([]byte{'x'}); err != nil {
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
				_ = trickle.Close()
			}()
			defer trickle.Close()
			go func() {
				response, err := client.Post(address+"/upload", "application/octet-stream", body)
				if err == nil {
					_ = response.Body.Close()
				}
			}()
			select {
			case err := <-readFailed:
				if !errors.Is(err, ErrBodyTooSlow) {
					t.Fatalf("the slow body read ended with %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a body far below the minimum rate is still being read")
			}
		})
	}
}

// TestBufferedResponseArrives checks that a short response, which the server
// still buffers when the handler returns, is sent whole.
func TestBufferedResponseArrives(t *testing.T) {
	address, client := serveUnderDeadlines(
		t,
		"HTTP/1.1",
		func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, "short answer")
		},
	)
	response, err := client.Get(address + "/short")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || string(answer) != "short answer" {
		t.Fatalf("the short answer: %d %q %v", response.StatusCode, answer, err)
	}
}

// TestSilentHandlerDoesNotHoldTheServer sends, over HTTP/1.1, a request that
// claims a one-byte body it never sends, to a handler that returns at once
// without writing or reading: the server's own read of the unread body after
// the handler must be bounded too, so the connection is answered or closed
// within a few body windows instead of waiting on the client forever.
func TestSilentHandlerDoesNotHoldTheServer(t *testing.T) {
	address, _ := serveUnderDeadlines(
		t,
		"HTTP/1.1",
		func(w http.ResponseWriter, req *http.Request) {},
	)
	conn, err := net.Dial("tcp", strings.TrimPrefix(address, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, unsentBodyRequest("/silent")); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	ended := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, conn)
		close(ended)
	}()
	select {
	case <-ended:
		if elapsed := time.Since(started); elapsed > 5*testProgress.BodyWindow {
			t.Fatalf("the connection ended after %s, want within a few body windows", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server still waits on a request body nobody reads")
	}
}

// TestDuplexReadAndWrite reads the body while writing the response on
// another goroutine, over HTTP/2, under -race: deadlines stay consistent and
// the echo completes.
func TestDuplexReadAndWrite(t *testing.T) {
	address, client := serveUnderDeadlines(
		t,
		"HTTP/2",
		func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			pieces := make(chan []byte, 16)
			go func() {
				defer close(pieces)
				piece := make([]byte, 4<<10)
				for {
					n, err := req.Body.Read(piece)
					if n > 0 {
						pieces <- append([]byte(nil), piece[:n]...)
					}
					if err != nil {
						return
					}
				}
			}()
			for piece := range pieces {
				if _, err := w.Write(piece); err != nil {
					return
				}
				_ = http.NewResponseController(w).Flush()
			}
		},
	)
	body := strings.Repeat("duplex ", 64<<10)
	response, err := client.Post(
		address+"/echo",
		"application/octet-stream",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	echoed, err := io.ReadAll(response.Body)
	if err != nil || string(echoed) != body {
		t.Fatalf("echoed %d of %d bytes: %v", len(echoed), len(body), err)
	}
}

// TestHijack takes the connection through http.Hijacker; the middleware then
// leaves it alone.
func TestHijack(t *testing.T) {
	address, _ := serveUnderDeadlines(
		t,
		"HTTP/1.1",
		func(w http.ResponseWriter, req *http.Request) {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("the response writer is not an http.Hijacker")
				return
			}
			conn, buffered, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_, _ = buffered.WriteString(
				"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nhijacked",
			)
			_ = buffered.Flush()
		},
	)
	response, err := http.Get(address + "/hijack")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	if err != nil || string(answer) != "hijacked" {
		t.Fatalf("the hijacked answer %q: %v", answer, err)
	}
}

// TestBodyDeadlinesWithoutWriteWindow repeats the silent handler with only
// the body window set, and with a handler that closes the unread body: the
// server's read of it is bounded either way.
func TestBodyDeadlinesWithoutWriteWindow(t *testing.T) {
	bodyOnly := &MiddlewareArgs{
		BodyWindow:  testProgress.BodyWindow,
		BodyBytes:   testProgress.BodyBytes,
		WriteWindow: -1,
	}
	for name, handler := range map[string]http.HandlerFunc{
		"silent":  func(w http.ResponseWriter, req *http.Request) {},
		"closing": func(w http.ResponseWriter, req *http.Request) { _ = req.Body.Close() },
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(Middleware(bodyOnly)(handler))
			t.Cleanup(server.Close)
			conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := io.WriteString(conn, unsentBodyRequest("/unread")); err != nil {
				t.Fatal(err)
			}
			ended := make(chan struct{})
			go func() {
				_, _ = io.Copy(io.Discard, conn)
				close(ended)
			}()
			select {
			case <-ended:
			case <-time.After(10 * time.Second):
				t.Fatal("the server still waits on a request body nobody sends")
			}
		})
	}
}

// testClock is a clock a test moves by hand.
type testClock struct{ current time.Time }

func (c *testClock) now() time.Time { return c.current }

// deadlineRecorder is a response writer that records the deadlines
// http.ResponseController sets on it, leaving out the clearing zero times.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	readDeadlines  []time.Time
	writeDeadlines []time.Time
}

func (r *deadlineRecorder) SetReadDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		r.readDeadlines = append(r.readDeadlines, deadline)
	}
	return nil
}

func (r *deadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		r.writeDeadlines = append(r.writeDeadlines, deadline)
	}
	return nil
}

// bodyDelivery is one read of a test body: how long the client takes to
// deliver it, and how many bytes it delivers.
type bodyDelivery struct {
	delay time.Duration
	bytes int
}

// clockedBody delivers its reads in order, moving the clock by each delay,
// and then io.EOF.
type clockedBody struct {
	clock      *testClock
	deliveries []bodyDelivery
}

func (b *clockedBody) Read(target []byte) (int, error) {
	if len(b.deliveries) == 0 {
		return 0, io.EOF
	}
	delivery := b.deliveries[0]
	b.deliveries = b.deliveries[1:]
	b.clock.current = b.clock.current.Add(delivery.delay)
	return copy(target, strings.Repeat("x", delivery.bytes)), nil
}

func (b *clockedBody) Close() error { return nil }

// TestDeadlinesFollowTheClock checks the deadline arithmetic on a clock the
// test moves: the time the handler spends between reads extends the body
// window rather than counting against the client, a window restarts once
// BodyBytes arrive, and each write's deadline is WriteWindow past it.
func TestDeadlinesFollowTheClock(t *testing.T) {
	started := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &testClock{current: started}
	body := &clockedBody{clock: clock, deliveries: []bodyDelivery{
		{delay: 2 * time.Second, bytes: 10},
		{delay: time.Second, bytes: 100},
		{delay: 0, bytes: 1},
	}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		target := make([]byte, 512)
		for range 2 {
			if _, err := req.Body.Read(target); err != nil {
				t.Errorf("body read: %v", err)
			}
			clock.current = clock.current.Add(5 * time.Second)
		}
		if _, err := io.ReadAll(req.Body); err != nil {
			t.Errorf("body read: %v", err)
		}
		if _, err := w.Write([]byte("done")); err != nil {
			t.Errorf("response write: %v", err)
		}
	})
	progress := newMinimumProgress(&MiddlewareArgs{
		BodyWindow:  10 * time.Second,
		BodyBytes:   100,
		WriteWindow: 30 * time.Second,
	})
	recorder := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodPost, "/", body)
	middleware(progress, clock.now)(handler).ServeHTTP(recorder, req)

	// The first read arms 0+10s and returns at 2s; the handler works 5s, so
	// the second read arms 15s, not 10s. It returns at 8s with the window's
	// 100 bytes, which starts a new window ending at 18s; the handler works
	// 5s more, so the third read, at 13s, arms 23s, and so does the read at
	// 13s that finds the end of the body.
	wantReads := []time.Duration{
		10 * time.Second,
		15 * time.Second,
		23 * time.Second,
		23 * time.Second,
	}
	gotReads := make([]time.Duration, 0, len(recorder.readDeadlines))
	for _, deadline := range recorder.readDeadlines {
		gotReads = append(gotReads, deadline.Sub(started))
	}
	if !slices.Equal(gotReads, wantReads) {
		t.Errorf("read deadlines = %v after the start, want %v", gotReads, wantReads)
	}
	// The write at 13s and the finish after the handler each arm 13+30s.
	wantWrites := []time.Duration{43 * time.Second, 43 * time.Second}
	gotWrites := make([]time.Duration, 0, len(recorder.writeDeadlines))
	for _, deadline := range recorder.writeDeadlines {
		gotWrites = append(gotWrites, deadline.Sub(started))
	}
	if !slices.Equal(gotWrites, wantWrites) {
		t.Errorf("write deadlines = %v after the start, want %v", gotWrites, wantWrites)
	}
}

// TestMiddlewareArgsDefaults checks that nil and zero fields ask for the
// defaults and a negative window turns its deadline off.
func TestMiddlewareArgsDefaults(t *testing.T) {
	defaults := minimumProgress{
		bodyWindow:  DefaultBodyWindow,
		bodyBytes:   DefaultBodyBytes,
		writeWindow: DefaultWriteWindow,
	}
	if got := newMinimumProgress(nil); got != defaults {
		t.Errorf("newMinimumProgress(nil) = %+v, want %+v", got, defaults)
	}
	if got := newMinimumProgress(&MiddlewareArgs{}); got != defaults {
		t.Errorf("newMinimumProgress(zero) = %+v, want %+v", got, defaults)
	}
	off := newMinimumProgress(&MiddlewareArgs{BodyWindow: -1, WriteWindow: -1})
	if off.bodyWindow != 0 || off.writeWindow != 0 {
		t.Errorf("negative windows = %+v, want both windows 0, no deadline", off)
	}
}

// unsentBodyRequest is an HTTP/1.1 POST to path whose header promises a
// 1-byte body that never follows.
func unsentBodyRequest(path string) string {
	return "POST " + path + " HTTP/1.1\r\nHost: test\r\nContent-Length: 1\r\n\r\n"
}
