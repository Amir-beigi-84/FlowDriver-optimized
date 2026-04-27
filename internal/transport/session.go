package transport

import (
	"sync"
	"time"
)

// Direction indicates if a file is req (client to server) or res (server to client)
type Direction string

const (
	DirReq Direction = "req"
	DirRes Direction = "res"
)

// Session represents an active proxy connection mapped to files.
type Session struct {
	ID           string
	mu           sync.Mutex
	txBuf        []byte
	txSeq        uint64
	txInFlight   bool
	rxSeq        uint64
	rxQueue      map[uint64]*Envelope
	lastActivity time.Time
	closed       bool
	rxClosed     bool // Safely tracks if RxChan was successfully closed
	TargetAddr   string
	ClientID     string

	// Backpressure: blocked when txBuf is too large
	txCond *sync.Cond

	// First-write optimization: delay empty open packet
	firstWritePending bool

	// App channel for receiving data downloaded from remote
	RxChan chan []byte
}

func NewSession(id string) *Session {
	s := &Session{
		ID:                id,
		rxQueue:           make(map[uint64]*Envelope),
		lastActivity:      time.Now(),
		firstWritePending: true,
		RxChan:            make(chan []byte, 1024),
	}
	s.txCond = sync.NewCond(&s.mu)
	return s
}

func (s *Session) EnqueueTx(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// BACKPRESSURE: Block if txBuf is larger than 2MB
	// This prevents memory explosion when uploading through the proxy
	for len(s.txBuf) > 2*1024*1024 && !s.closed {
		s.txCond.Wait()
	}

	s.txBuf = append(s.txBuf, data...)
	s.lastActivity = time.Now()
	s.firstWritePending = false
}

func (s *Session) ClearTx() {
	s.mu.Lock()
	s.txBuf = nil
	s.txInFlight = false
	s.txCond.Broadcast() // Wake up any writers blocked on backpressure
	s.mu.Unlock()
}

func (s *Session) ProcessRx(env *Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()

	if s.rxClosed {
		return // Ignore packets if the channel is already safely closed
	}

	if env.Seq == s.rxSeq {
		if len(env.Payload) > 0 {
			s.RxChan <- env.Payload
		}
		s.rxSeq++
		if env.Close {
			s.rxClosed = true
			s.closed = true
			close(s.RxChan)
			return
		}

		// process any queued future packets
		for {
			if nextEnv, ok := s.rxQueue[s.rxSeq]; ok {
				if len(nextEnv.Payload) > 0 {
					s.RxChan <- nextEnv.Payload
				}
				delete(s.rxQueue, s.rxSeq)
				s.rxSeq++
				if nextEnv.Close {
					s.rxClosed = true
					s.closed = true
					close(s.RxChan)
					return
				}
			} else {
				break
			}
		}
	} else if env.Seq > s.rxSeq {
		s.rxQueue[env.Seq] = env
	}
}
