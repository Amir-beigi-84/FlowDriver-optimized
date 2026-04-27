package storage

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/NullLatency/flow-driver/internal/metrics"
)

type metricsBackend struct {
	next Backend
}

type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.n.Add(int64(n))
	}
	return n, err
}

type countingReadCloser struct {
	rc      io.ReadCloser
	n       atomic.Int64
	onClose func(int64)
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if n > 0 {
		r.n.Add(int64(n))
	}
	return n, err
}

func (r *countingReadCloser) Close() error {
	err := r.rc.Close()
	r.onClose(r.n.Load())
	return err
}

// WithMetrics records storage operation counts, latencies, and byte totals.
func WithMetrics(next Backend) Backend {
	return &metricsBackend{next: next}
}

func (b *metricsBackend) Login(ctx context.Context) error {
	return b.next.Login(ctx)
}

func (b *metricsBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	start := time.Now()
	counter := &countingReader{r: data}
	err := b.next.Upload(ctx, filename, counter)
	metrics.Global().RecordUpload(counter.n.Load(), time.Since(start))
	return err
}

func (b *metricsBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	start := time.Now()
	files, err := b.next.ListQuery(ctx, prefix)
	metrics.Global().RecordList(time.Since(start))
	return files, err
}

func (b *metricsBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := b.next.Download(ctx, filename)
	metrics.Global().RecordDownloadOpen(time.Since(start))
	if err != nil {
		return nil, err
	}
	return &countingReadCloser{
		rc: rc,
		onClose: func(bytes int64) {
			metrics.Global().RecordDownloadBytes(bytes)
		},
	}, nil
}

func (b *metricsBackend) Delete(ctx context.Context, filename string) error {
	start := time.Now()
	err := b.next.Delete(ctx, filename)
	metrics.Global().RecordDelete(time.Since(start))
	return err
}

func (b *metricsBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	return b.next.CreateFolder(ctx, name)
}

func (b *metricsBackend) FindFolder(ctx context.Context, name string) (string, error) {
	return b.next.FindFolder(ctx, name)
}
