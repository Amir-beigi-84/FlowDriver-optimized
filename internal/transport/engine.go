package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NullLatency/flow-driver/internal/metrics"
	"github.com/NullLatency/flow-driver/internal/storage"
)

type txSnapshot struct {
	session    *Session
	payload    []byte
	closed     bool
	enqueuedAt time.Time
}

func (snap txSnapshot) commit() (removeSession bool, hasMoreData bool) {
	s := snap.session

	s.mu.Lock()
	defer s.mu.Unlock()

	s.txSeq++
	s.txInFlight = false
	s.txCond.Broadcast()

	return snap.closed, len(s.txBuf) > 0
}

func (snap txSnapshot) rollback() {
	s := snap.session

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(snap.payload) > 0 {
		restored := make([]byte, 0, len(snap.payload)+len(s.txBuf))
		restored = append(restored, snap.payload...)
		restored = append(restored, s.txBuf...)
		s.txBuf = restored
		s.txEnqueuedAt = snap.enqueuedAt
	}

	s.txInFlight = false
	s.txCond.Broadcast()
}

// Engine manages the local sessions, periodically flushes Tx buffers to files,
// and polls for new Rx files.
type Engine struct {
	backend storage.Backend
	myDir   Direction // DirReq for client, DirRes for server
	peerDir Direction // DirRes for client, DirReq for server
	id      string    // ClientID for client, empty for server

	sessions  map[string]*Session
	sessionMu sync.RWMutex

	// Tombstones for recently closed sessions to prevent re-triggering on delayed packets
	closedSessions   map[string]time.Time
	closedSessionsMu sync.Mutex

	pollTicker  time.Duration
	flushTicker time.Duration

	// Adaptive polling: fast when active, slow when idle
	pollTickerIdle   time.Duration
	pollTickerActive time.Duration
	pollIdleMax      time.Duration
	activePollWindow time.Duration
	activeUntil      atomic.Int64

	flushNow      chan struct{}
	pollNow       chan struct{}
	flushCoalesce time.Duration

	// Server mode handler: called when a new session is discovered
	OnNewSession func(sessionID, targetAddr string, s *Session)

	// Concurrency control for storage operations (Upload/Download)
	sem chan struct{}

	// Track processed files to avoid duplicates
	processed   map[string]bool
	processedMu sync.Mutex

	wg sync.WaitGroup
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	e := &Engine{
		backend:        backend,
		id:             clientID,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		processed:      make(map[string]bool),
		// Default intervals
		pollTicker:       time.Second,
		pollTickerIdle:   time.Second,
		pollTickerActive: 75 * time.Millisecond,
		pollIdleMax:      5 * time.Second,
		activePollWindow: 15 * time.Second,
		flushTicker:      300 * time.Millisecond,
		flushNow:         make(chan struct{}, 1),
		pollNow:          make(chan struct{}, 1),
		flushCoalesce:    20 * time.Millisecond,
	}
	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}
	// Limit to 8 concurrent upload/download operations to avoid OOM and FD exhaustion
	e.sem = make(chan struct{}, 8)
	return e
}

func (e *Engine) SetRefreshRate(ms int) {
	if ms > 0 {
		e.SetPollRate(ms)
		// Legacy behavior: sets both if FlushTicker was still at default
		if e.flushTicker == 300*time.Millisecond {
			e.flushTicker = time.Duration(ms) * time.Millisecond
		}
	}
}

func (e *Engine) SetPollRate(ms int) {
	if ms > 0 {
		idle := time.Duration(ms) * time.Millisecond
		active := idle / 4
		if active < 50*time.Millisecond {
			active = 50 * time.Millisecond
		}
		if active > 100*time.Millisecond {
			active = 100 * time.Millisecond
		}
		e.pollTicker = idle
		e.pollTickerIdle = idle
		e.pollTickerActive = active
		e.pollIdleMax = idle
	}
}

func (e *Engine) SetAdaptivePoll(idleMs, activeMs, activeWindowMs int) {
	if idleMs > 0 {
		e.pollTickerIdle = time.Duration(idleMs) * time.Millisecond
		e.pollTicker = e.pollTickerIdle
		e.pollIdleMax = 5 * time.Second
	}
	if activeMs > 0 {
		e.pollTickerActive = time.Duration(activeMs) * time.Millisecond
	}
	if activeWindowMs > 0 {
		e.activePollWindow = time.Duration(activeWindowMs) * time.Millisecond
	}
}

func (e *Engine) SetFlushRate(ms int) {
	if ms > 0 {
		e.flushTicker = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) markActive() {
	if e.activePollWindow > 0 {
		e.activeUntil.Store(time.Now().Add(e.activePollWindow).UnixNano())
	}
	e.wakePoll()
}

func (e *Engine) recentlyActive() bool {
	deadline := e.activeUntil.Load()
	return deadline > 0 && time.Now().UnixNano() < deadline
}

func (e *Engine) wakePoll() {
	select {
	case e.pollNow <- struct{}{}:
	default:
	}
}

func (e *Engine) Start(ctx context.Context) {
	go e.flushLoop(ctx)
	go e.pollLoop(ctx)
	go e.cleanupLoop(ctx) // Delete files older than 10s
}

func (e *Engine) GetSession(id string) *Session {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return e.sessions[id]
}

func (e *Engine) AddSession(s *Session) {
	e.sessionMu.Lock()
	defer e.sessionMu.Unlock()
	e.sessions[s.ID] = s
	metrics.Global().SetActiveSessions(int64(len(e.sessions)))
	e.markActive()
	log.Printf("Engine.AddSession: Added session %s (Total now: %d)", s.ID, len(e.sessions))
}

func (e *Engine) RequestFlush() {
	e.markActive()
	select {
	case e.flushNow <- struct{}{}:
	default:
	}
}

func (e *Engine) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(e.flushTicker)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			e.flushAll(ctx)

		case <-e.flushNow:
			timer := time.NewTimer(e.flushCoalesce)

			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}

			e.flushAll(ctx)
		}
	}
}

func (e *Engine) flushAll(ctx context.Context) {
	e.sessionMu.RLock()
	metrics.Global().SetActiveSessions(int64(len(e.sessions)))
	e.sessionMu.RUnlock()

	e.sessionMu.Lock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.Unlock()

	muxes := make(map[string][]Envelope)
	snapshots := make(map[string][]txSnapshot)

	for _, s := range sessions {
		s.mu.Lock()

		// Idle Timeout check
		if time.Since(s.lastActivity) > 10*time.Second {
			s.closed = true
		}

		if s.txInFlight {
			s.mu.Unlock()
			continue
		}

		// Decide if we should send a packet
		shouldSend := false
		if len(s.txBuf) > 0 {
			shouldSend = true
		} else if s.closed {
			shouldSend = true
		} else if s.txSeq == 0 && e.myDir == DirReq {
			// Only send empty open if first write hasn't arrived within delay window
			if !s.firstWritePending || time.Since(s.lastActivity) > 25*time.Millisecond {
				shouldSend = true
			}
		}
		if !shouldSend {
			s.mu.Unlock()
			continue
		}

		payload := append([]byte(nil), s.txBuf...)
		enqueuedAt := s.txEnqueuedAt
		s.txBuf = nil
		s.txEnqueuedAt = time.Time{}
		s.txInFlight = true
		s.txCond.Broadcast()

		env := Envelope{
			SessionID:  s.ID,
			Seq:        s.txSeq,
			Payload:    payload,
			Close:      s.closed,
			TargetAddr: s.TargetAddr,
		}

		cid := s.ClientID
		if cid == "" && e.myDir == DirReq {
			cid = e.id // For client requests, use our own ID
		}
		if cid == "" {
			cid = "unknown"
		}

		muxes[cid] = append(muxes[cid], env)
		snapshots[cid] = append(snapshots[cid], txSnapshot{
			session:    s,
			payload:    payload,
			closed:     s.closed,
			enqueuedAt: enqueuedAt,
		})

		s.mu.Unlock()
	}

	for cid, mux := range muxes {
		var payloadBytes int64
		for _, env := range mux {
			payloadBytes += int64(len(env.Payload))
		}
		metrics.Global().RecordPayloadBytes(payloadBytes)

		filename := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, cid, time.Now().UnixNano())
		go e.uploadMux(ctx, filename, mux, snapshots[cid])
	}
}

func (e *Engine) uploadMux(ctx context.Context, filename string, mux []Envelope, snapshots []txSnapshot) {
	e.sem <- struct{}{}
	defer func() { <-e.sem }()

	pr, pw := io.Pipe()

	go func() {
		for _, env := range mux {
			if err := env.Encode(pw); err != nil {
				_ = pw.CloseWithError(err)
				log.Printf("mux encode error %s: %v", filename, err)
				return
			}
		}

		_ = pw.Close()
	}()

	if err := e.backend.Upload(ctx, filename, pr); err != nil {
		log.Printf("upload error %s: %v", filename, err)

		for _, snap := range snapshots {
			snap.rollback()
		}

		e.RequestFlush()
		return
	}
	e.markActive()

	hasMoreData := false

	for _, snap := range snapshots {
		if !snap.enqueuedAt.IsZero() {
			metrics.Global().RecordEnqueueTx(time.Since(snap.enqueuedAt))
		}

		removeSession, moreData := snap.commit()

		if removeSession {
			e.RemoveSession(snap.session.ID)
			continue
		}

		if moreData {
			hasMoreData = true
		}
	}

	if hasMoreData {
		e.RequestFlush()
	}
}

func (e *Engine) pollLoop(ctx context.Context) {
	currentPollInterval := e.pollTicker
	timer := time.NewTimer(currentPollInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-e.pollNow:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

	pollAgain:
		// ZERO-TRAFFIC CLIENT OPTIMIZATION:
		// SOCKS5 only initiates from the Client. If the Client has 0 active sessions,
		// it mathematically never needs to poll Google Drive! Go entirely to sleep!
		if e.myDir == DirReq {
			e.sessionMu.RLock()
			count := len(e.sessions)
			e.sessionMu.RUnlock()
			if count == 0 {
				currentPollInterval = nextIdlePollInterval(currentPollInterval, e.pollTickerIdle, e.pollIdleMax)
				timer.Reset(currentPollInterval)
				continue
			}
		}

		// Fetch multiplexed files
		prefix := string(e.peerDir) + "-"
		if e.myDir == DirReq {
			// Client only polls for its own responses
			prefix += e.id + "-mux-"
		} else {
			// Server polls for ALL client requests
			prefix += ""
		}
		files, err := e.backend.ListQuery(ctx, prefix)
		if err != nil {
			log.Printf("poll list error: %v", err)
			timer.Reset(currentPollInterval)
			continue
		}

		metrics.Global().RecordPoll(len(files))

		if len(files) == 0 {
			e.sessionMu.RLock()
			activeSessions := len(e.sessions)
			e.sessionMu.RUnlock()

			if activeSessions > 0 || e.recentlyActive() {
				currentPollInterval = e.pollTickerActive
			} else {
				currentPollInterval = nextIdlePollInterval(currentPollInterval, e.pollTickerIdle, e.pollIdleMax)
			}
			timer.Reset(currentPollInterval)
			continue
		}

		// We found data! Reset polling back to maximum speed
		e.markActive()
		currentPollInterval = e.pollTickerActive

		// We found files! Let's download them in parallel to boost speed massively
		var wg sync.WaitGroup
		for _, f := range files {
			// STARTUP OPTIMIZATION: Ignore files older than 5 minutes to avoid memory spikes on restart
			parts := strings.Split(f, "-")
			if len(parts) >= 3 {
				tsStr := parts[len(parts)-1]
				tsStr = strings.TrimSuffix(tsStr, ".bin")
				ts, _ := strconv.ParseInt(tsStr, 10, 64)
				if ts > 0 && time.Since(time.Unix(0, ts)) > 5*time.Minute {
					e.backend.Delete(ctx, f) // Silent cleanup
					continue
				}
			}

			e.processedMu.Lock()
			already := e.processed[f]
			if !already {
				e.processed[f] = true
			}
			e.processedMu.Unlock()

			if already {
				continue
			}

			wg.Add(1)
			go func(fname string) {
				defer wg.Done()

				e.sem <- struct{}{}        // Acquire
				defer func() { <-e.sem }() // Release

				// log.Printf("Engine.pollLoop: Downloading %s", fname)
				rc, err := e.backend.Download(ctx, fname)
				if err != nil {
					log.Printf("download error %s: %v", fname, err)
					e.processedMu.Lock()
					delete(e.processed, fname) // failed to download, retry next poll
					e.processedMu.Unlock()
					return
				}
				e.markActive()
				defer rc.Close()
				downloadDone := time.Now()

				// Extract ClientID from filename for server-side session initialization
				var fileClientID string
				parts := strings.Split(fname, "-")
				if len(parts) >= 4 && parts[2] == "mux" {
					fileClientID = parts[1]
				}

				// STREAMING DECODE
				count := 0
				for {
					var env Envelope
					if err := env.Decode(rc); err != nil {
						if err != io.EOF && err != io.ErrUnexpectedEOF {
							log.Printf("mux decode error %s: %v", fname, err)
						}
						break
					}
					count++

					// Process envelope immediately
					e.closedSessionsMu.Lock()
					if _, exists := e.closedSessions[env.SessionID]; exists {
						e.closedSessionsMu.Unlock()
						continue
					}
					e.closedSessionsMu.Unlock()

					e.sessionMu.Lock()
					s, exists := e.sessions[env.SessionID]
					if !exists && e.myDir == DirRes && e.OnNewSession != nil {
						s = NewSession(env.SessionID)
						s.ClientID = fileClientID
						e.sessions[env.SessionID] = s
						e.sessionMu.Unlock()
						log.Printf("Engine: Triggering new session %s for Client %s", env.SessionID, fileClientID)
						e.OnNewSession(env.SessionID, env.TargetAddr, s)
					} else {
						e.sessionMu.Unlock()
					}

					if s != nil {
						s.ProcessRx(&env)
						metrics.Global().RecordDownloadToRx(time.Since(downloadDone))
					}
				}

				e.backend.Delete(ctx, fname)
			}(f)
		}

		// Wait for parallel batch to finish
		wg.Wait()

		// Adaptive Polling: Because we just received data, the connection is active.
		// Instead of jumping back to the select, immediately poll again after a tiny 100ms break to drain queues.
		time.Sleep(5 * time.Millisecond)
		goto pollAgain
	}
}

func nextIdlePollInterval(current, idle, max time.Duration) time.Duration {
	if idle <= 0 {
		idle = time.Second
	}
	if max <= 0 {
		max = 5 * time.Second
	}
	if current < idle {
		return idle
	}
	next := current * 2
	if next < idle {
		next = idle
	}
	if next > max {
		next = max
	}
	return next
}

// CloseSession marks session closed and triggers immediate flush.
// Does not remove session - call RemoveSession after final flush completes.
func (e *Engine) CloseSession(id string) {
	e.sessionMu.RLock()
	s := e.sessions[id]
	e.sessionMu.RUnlock()

	if s == nil {
		return
	}

	s.mu.Lock()
	s.closed = true
	s.txCond.Broadcast()
	s.mu.Unlock()

	e.RequestFlush()
}

// CloseAndFlush marks session closed, flushes final data, then removes session.
// Blocks until final flush completes or context cancelled.
func (e *Engine) CloseAndFlush(ctx context.Context, id string) {
	e.sessionMu.RLock()
	s := e.sessions[id]
	e.sessionMu.RUnlock()

	if s == nil {
		return
	}

	s.mu.Lock()
	s.closed = true
	s.txCond.Broadcast()
	s.mu.Unlock()

	// Trigger immediate flush
	e.RequestFlush()

	// Wait briefly for flush to complete
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}

	e.RemoveSession(id)
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	delete(e.sessions, id)
	metrics.Global().SetActiveSessions(int64(len(e.sessions)))
	e.sessionMu.Unlock()

	// Add to tombstone list
	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
}

func (e *Engine) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Cleanup old tombstones (older than 30s)
			e.closedSessionsMu.Lock()
			for id, t := range e.closedSessions {
				if time.Since(t) > 30*time.Second {
					delete(e.closedSessions, id)
				}
			}
			e.closedSessionsMu.Unlock()

			// Periodically clear processed map to prevent infinite growth
			e.processedMu.Lock()
			if len(e.processed) > 5000 {
				e.processed = make(map[string]bool)
			}
			e.processedMu.Unlock()

			// ZERO-TRAFFIC CLIENT OPTIMIZATION:
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					continue
				}
			}

			files, _ := e.backend.ListQuery(ctx, string(e.myDir)+"-")
			for _, f := range files {
				parts := strings.Split(f, "-")
				// Formats:
				// OLD: "req", "UUID...", "Seq", "Timestamp.json" (len >= 4)
				// MUX: "req", "mux", "Timestamp.json" (len >= 3)
				if len(parts) >= 3 {
					tsStr := parts[len(parts)-1]
					tsStr = strings.TrimSuffix(tsStr, ".json")
					tsStr = strings.TrimSuffix(tsStr, ".bin")
					ts, err := strconv.ParseInt(tsStr, 10, 64)
					if err == nil {
						t := time.Unix(0, ts)
						if time.Since(t) > 10*time.Second {
							e.backend.Delete(ctx, f)
						}
					}
				}
			}
		}
	}
}
