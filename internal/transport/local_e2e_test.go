package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

func TestLocalEngineEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()

	clientBackend, err := storage.NewLocalBackend(tmpDir)
	if err != nil {
		t.Fatalf("client local backend: %v", err)
	}

	serverBackend, err := storage.NewLocalBackend(tmpDir)
	if err != nil {
		t.Fatalf("server local backend: %v", err)
	}

	targetAddr, stopTarget := startTestTarget(t)
	defer stopTarget()

	clientEngine := NewEngine(clientBackend, true, "dev1")
	serverEngine := NewEngine(serverBackend, false, "")

	// Keep test fast. Event-driven flush should do most of the work,
	// but fast polling makes the local backend E2E test deterministic.
	clientEngine.SetAdaptivePoll(10, 10, 1000)
	serverEngine.SetAdaptivePoll(10, 10, 1000)
	clientEngine.flushTicker = time.Hour
	serverEngine.flushTicker = time.Hour
	clientEngine.flushCoalesce = time.Millisecond
	serverEngine.flushCoalesce = time.Millisecond

	proxyErrCh := make(chan error, 4)

	serverEngine.OnNewSession = func(sessionID, targetAddr string, session *Session) {
		go testProxySession(targetAddr, session, serverEngine, proxyErrCh)
	}

	serverEngine.Start(ctx)
	clientEngine.Start(ctx)

	session := NewSession("sess-local-e2e")
	session.TargetAddr = targetAddr
	clientEngine.AddSession(session)

	conn := NewVirtualConn(session, clientEngine)

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("client write failed: %v", err)
	}

	readDone := make(chan struct {
		n   int
		err error
		buf []byte
	}, 1)

	go func() {
		buf := make([]byte, 4)
		n, err := io.ReadFull(conn, buf)
		readDone <- struct {
			n   int
			err error
			buf []byte
		}{n: n, err: err, buf: buf}
	}()

	select {
	case result := <-readDone:
		if result.err != nil {
			t.Fatalf("client read failed: %v", result.err)
		}
		if got := string(result.buf[:result.n]); got != "pong" {
			t.Fatalf("client read %q, want %q", got, "pong")
		}

	case err := <-proxyErrCh:
		t.Fatalf("server proxy failed before response: %v", err)

	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for local engine E2E response")
	}
}

func startTestTarget(t *testing.T) (addr string, stop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen test target: %v", err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}

		if string(buf) != "ping" {
			return
		}

		_, _ = conn.Write([]byte("pong"))
	}()

	stop = func() {
		_ = ln.Close()

		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}

	return ln.Addr().String(), stop
}

func testProxySession(targetAddr string, session *Session, engine *Engine, errCh chan<- error) {
	defer engine.CloseAndFlush(context.Background(), session.ID)

	conn, err := net.DialTimeout("tcp", targetAddr, time.Second)
	if err != nil {
		errCh <- err
		return
	}
	defer conn.Close()

	done := make(chan error, 2)

	go func() {
		buf := make([]byte, 4096)

		for {
			n, err := conn.Read(buf)
			if n > 0 {
				session.EnqueueTx(buf[:n])
				engine.RequestFlush()
			}
			if err != nil {
				done <- err
				return
			}
		}
	}()

	go func() {
		for data := range session.RxChan {
			if len(data) == 0 {
				continue
			}

			if _, err := conn.Write(data); err != nil {
				done <- err
				return
			}
		}

		done <- io.EOF
	}()

	err = <-done
	if err != nil && err != io.EOF {
		errCh <- err
	}
}
