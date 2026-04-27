package metrics

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks transport and storage operation counters and latencies.
type Metrics struct {
	// Storage operation counters
	uploadsTotal     atomic.Int64
	downloadsTotal   atomic.Int64
	listCallsTotal   atomic.Int64
	deleteCallsTotal atomic.Int64

	// Bytes transferred
	uploadBytes   atomic.Int64
	downloadBytes atomic.Int64

	// Poll efficiency
	emptyPolls    atomic.Int64
	nonEmptyPolls atomic.Int64
	filesPerPoll  atomic.Int64
	pollCount     atomic.Int64

	// Latency tracking (microseconds)
	uploadLatencySum   atomic.Int64
	listLatencySum     atomic.Int64
	downloadLatencySum atomic.Int64
	deleteLatencySum   atomic.Int64

	// Enqueue to upload latency
	enqueueTxSum   atomic.Int64
	enqueueTxCount atomic.Int64

	// Download to ProcessRx latency
	downloadToRxSum   atomic.Int64
	downloadToRxCount atomic.Int64

	// Active sessions
	activeSessions atomic.Int64

	// Payload stats
	payloadBytesSum   atomic.Int64
	payloadBytesCount atomic.Int64

	startTime time.Time
	mu        sync.Mutex
}

var global = &Metrics{
	startTime: time.Now(),
}

// Global returns the global metrics instance.
func Global() *Metrics {
	return global
}

// RecordUpload increments upload counter and latency.
func (m *Metrics) RecordUpload(bytes int64, latency time.Duration) {
	m.uploadsTotal.Add(1)
	m.uploadBytes.Add(bytes)
	m.uploadLatencySum.Add(latency.Microseconds())
}

// RecordDownloadOpen increments download counter and records request latency.
func (m *Metrics) RecordDownloadOpen(latency time.Duration) {
	m.downloadsTotal.Add(1)
	m.downloadLatencySum.Add(latency.Microseconds())
}

// RecordDownloadBytes records bytes read from downloaded files.
func (m *Metrics) RecordDownloadBytes(bytes int64) {
	m.downloadBytes.Add(bytes)
}

// RecordList increments list call counter and latency.
func (m *Metrics) RecordList(latency time.Duration) {
	m.listCallsTotal.Add(1)
	m.listLatencySum.Add(latency.Microseconds())
}

// RecordDelete increments delete counter and latency.
func (m *Metrics) RecordDelete(latency time.Duration) {
	m.deleteCallsTotal.Add(1)
	m.deleteLatencySum.Add(latency.Microseconds())
}

// RecordPoll records poll result.
func (m *Metrics) RecordPoll(fileCount int) {
	m.pollCount.Add(1)
	if fileCount == 0 {
		m.emptyPolls.Add(1)
	} else {
		m.nonEmptyPolls.Add(1)
		m.filesPerPoll.Add(int64(fileCount))
	}
}

// RecordEnqueueTx records time from enqueue to upload.
func (m *Metrics) RecordEnqueueTx(latency time.Duration) {
	m.enqueueTxSum.Add(latency.Microseconds())
	m.enqueueTxCount.Add(1)
}

// RecordDownloadToRx records time from download to ProcessRx.
func (m *Metrics) RecordDownloadToRx(latency time.Duration) {
	m.downloadToRxSum.Add(latency.Microseconds())
	m.downloadToRxCount.Add(1)
}

// SetActiveSessions sets current active session count.
func (m *Metrics) SetActiveSessions(count int64) {
	m.activeSessions.Store(count)
}

// RecordPayloadBytes records mux file payload size.
func (m *Metrics) RecordPayloadBytes(bytes int64) {
	m.payloadBytesSum.Add(bytes)
	m.payloadBytesCount.Add(1)
}

// LogSummary prints metrics summary.
func (m *Metrics) LogSummary() {
	uploads := m.uploadsTotal.Load()
	downloads := m.downloadsTotal.Load()
	lists := m.listCallsTotal.Load()
	deletes := m.deleteCallsTotal.Load()

	uploadBytes := m.uploadBytes.Load()
	downloadBytes := m.downloadBytes.Load()

	emptyPolls := m.emptyPolls.Load()
	nonEmptyPolls := m.nonEmptyPolls.Load()
	pollCount := m.pollCount.Load()

	activeSessions := m.activeSessions.Load()

	var avgUploadMs, avgListMs, avgDownloadMs, avgDeleteMs float64
	var avgEnqueueMs, avgDownloadToRxMs float64
	var avgFilesPerPoll, avgPayloadBytes float64

	if uploads > 0 {
		avgUploadMs = float64(m.uploadLatencySum.Load()) / float64(uploads) / 1000.0
	}
	if lists > 0 {
		avgListMs = float64(m.listLatencySum.Load()) / float64(lists) / 1000.0
	}
	if downloads > 0 {
		avgDownloadMs = float64(m.downloadLatencySum.Load()) / float64(downloads) / 1000.0
	}
	if deletes > 0 {
		avgDeleteMs = float64(m.deleteLatencySum.Load()) / float64(deletes) / 1000.0
	}

	enqueueTxCount := m.enqueueTxCount.Load()
	if enqueueTxCount > 0 {
		avgEnqueueMs = float64(m.enqueueTxSum.Load()) / float64(enqueueTxCount) / 1000.0
	}

	downloadToRxCount := m.downloadToRxCount.Load()
	if downloadToRxCount > 0 {
		avgDownloadToRxMs = float64(m.downloadToRxSum.Load()) / float64(downloadToRxCount) / 1000.0
	}

	if nonEmptyPolls > 0 {
		avgFilesPerPoll = float64(m.filesPerPoll.Load()) / float64(nonEmptyPolls)
	}

	payloadBytesCount := m.payloadBytesCount.Load()
	if payloadBytesCount > 0 {
		avgPayloadBytes = float64(m.payloadBytesSum.Load()) / float64(payloadBytesCount)
	}

	uptime := time.Since(m.startTime)

	log.Printf("[METRICS] Uptime: %v | Active Sessions: %d", uptime.Round(time.Second), activeSessions)
	log.Printf("[METRICS] Storage Ops: Upload=%d List=%d Download=%d Delete=%d", uploads, lists, downloads, deletes)
	log.Printf("[METRICS] Bytes: Upload=%d Download=%d", uploadBytes, downloadBytes)
	log.Printf("[METRICS] Polls: Total=%d Empty=%d NonEmpty=%d AvgFiles=%.1f", pollCount, emptyPolls, nonEmptyPolls, avgFilesPerPoll)
	log.Printf("[METRICS] Latency(ms): Upload=%.1f List=%.1f Download=%.1f Delete=%.1f", avgUploadMs, avgListMs, avgDownloadMs, avgDeleteMs)
	log.Printf("[METRICS] E2E(ms): EnqueueToUpload=%.1f DownloadToRx=%.1f", avgEnqueueMs, avgDownloadToRxMs)
	log.Printf("[METRICS] Payload: AvgBytes=%.0f", avgPayloadBytes)
}

// StartPeriodicLog starts periodic metrics logging.
func (m *Metrics) StartPeriodicLog(interval time.Duration) {
	if interval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			m.LogSummary()
		}
	}()
}
