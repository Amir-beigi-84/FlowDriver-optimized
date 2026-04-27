package transport

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

type captureBackend struct {
	uploads chan []byte
}

func newCaptureBackend() *captureBackend {
	return &captureBackend{uploads: make(chan []byte, 4)}
}

func (b *captureBackend) Login(ctx context.Context) error { return nil }

func (b *captureBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	buf, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	b.uploads <- buf
	return nil
}

func (b *captureBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}

func (b *captureBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}

func (b *captureBackend) Delete(ctx context.Context, filename string) error { return nil }

func (b *captureBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	return "", nil
}

func (b *captureBackend) FindFolder(ctx context.Context, name string) (string, error) {
	return "", nil
}

func TestFirstWritePiggybacksOnOpen(t *testing.T) {
	backend := newCaptureBackend()
	engine := NewEngine(backend, true, "dev1")
	session := NewSession("sess-first-write")
	session.TargetAddr = "example.com:80"
	engine.AddSession(session)

	session.EnqueueTx([]byte("GET / HTTP/1.1\r\n\r\n"))
	engine.flushAll(context.Background())

	env := readSingleUploadedEnvelope(t, backend)
	if env.Seq != 0 {
		t.Fatalf("seq = %d, want 0", env.Seq)
	}
	if got := string(env.Payload); got != "GET / HTTP/1.1\r\n\r\n" {
		t.Fatalf("payload = %q", got)
	}
	if env.TargetAddr != "example.com:80" {
		t.Fatalf("target addr = %q", env.TargetAddr)
	}
}

func TestEmptyOpenAfterFirstOpenDelay(t *testing.T) {
	backend := newCaptureBackend()
	engine := NewEngine(backend, true, "dev1")
	engine.SetFirstOpenDelay(5)
	session := NewSession("sess-empty-open")
	session.TargetAddr = "example.com:80"
	engine.AddSession(session)

	time.Sleep(10 * time.Millisecond)
	engine.flushAll(context.Background())

	env := readSingleUploadedEnvelope(t, backend)
	if env.Seq != 0 {
		t.Fatalf("seq = %d, want 0", env.Seq)
	}
	if len(env.Payload) != 0 {
		t.Fatalf("payload len = %d, want 0", len(env.Payload))
	}
	if env.TargetAddr != "example.com:80" {
		t.Fatalf("target addr = %q", env.TargetAddr)
	}
}

func readSingleUploadedEnvelope(t *testing.T, backend *captureBackend) Envelope {
	t.Helper()

	select {
	case data := <-backend.uploads:
		var env Envelope
		if err := env.Decode(bytes.NewReader(data)); err != nil {
			t.Fatalf("decode uploaded envelope: %v", err)
		}
		return env
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for upload")
	}

	return Envelope{}
}
