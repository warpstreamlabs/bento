package grpc_client

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// ConnectionPool manages a pool of gRPC connections for performance optimization.
//
// The pool implements a round-robin connection selection strategy with automatic
// connection release. Connections are marked as "in use" temporarily to prevent
// concurrent access conflicts, then automatically released after a short delay.
//
// Thread Safety: All public methods are thread-safe using RWMutex protection.
// The pool supports concurrent access from multiple goroutines.
//
// Lifecycle: Connections are created during pool initialization and replaced
// when they become idle beyond the configured timeout.
type ConnectionPool struct {
	connections []connectionEntry // Pool of gRPC connections
	mu          sync.RWMutex      // Protects concurrent access to pool state
	cfg         *Config           // Configuration for connection management
	nextIndex   int               // Round-robin index for connection selection
	closed      bool              // Indicates if pool is closed
	ctx         context.Context   // Parent context for new connections
}

// connectionEntry represents a single gRPC connection in the pool with metadata
type connectionEntry struct {
	conn          *grpc.ClientConn // The actual gRPC connection
	lastUsed      time.Time        // Timestamp of last usage for idle cleanup
	createdAt     time.Time        // When this connection was created
	failureCount  int              // Number of consecutive failures
	lastFailure   time.Time        // Time of last failure
	healthChecked time.Time        // Last time health was checked
}

// getConnection gets an available connection from the pool with state validation
func (cp *ConnectionPool) getConnection() (*grpc.ClientConn, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return nil, errors.New("connection pool is closed")
	}
	for i := 0; i < len(cp.connections); i++ {
		idx := (cp.nextIndex + i) % len(cp.connections)
		entry := &cp.connections[idx]
		if !isConnectionExcessivelyFailing(entry, defaultMaxConnectionFailures, defaultFailureWindow) {
			if !isConnectionHealthy(entry.conn) {
				recordConnectionFailure(entry)
				if newConn, err := createConnection(cp.ctx, cp.cfg); err == nil {
					entry.conn.Close()
					now := time.Now()
					entry.conn = newConn
					entry.lastUsed = now
					entry.createdAt = now
					entry.failureCount = 0
					entry.lastFailure = time.Time{}
					entry.healthChecked = now
				} else {
					continue
				}
			}
			entry.lastUsed = time.Now()
			entry.healthChecked = time.Now()
			cp.nextIndex = (idx + 1) % len(cp.connections)
			recordConnectionSuccess(entry)
			return entry.conn, nil
		}
	}
	return nil, errors.New("no connections available in pool")
}

// cleanupIdle removes connections that have been idle for too long or are unhealthy
func (cp *ConnectionPool) cleanupIdle() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.closed {
		return
	}
	now := time.Now()
	for i := range cp.connections {
		entry := &cp.connections[i]
		shouldReplace := false
		if now.Sub(entry.lastUsed) > defaultConnectionIdleTimeout {
			shouldReplace = true
		}
		if isConnectionExcessivelyFailing(entry, defaultMaxConnectionFailures, defaultFailureWindow) {
			shouldReplace = true
		}
		if now.Sub(entry.healthChecked) > defaultHealthCheckInterval {
			if !isConnectionHealthy(entry.conn) {
				recordConnectionFailure(entry)
				shouldReplace = true
			} else {
				entry.healthChecked = now
				recordConnectionSuccess(entry)
			}
		}
		if shouldReplace {
			if entry.conn != nil {
				entry.conn.Close()
			}
			if newConn, err := createConnection(cp.ctx, cp.cfg); err == nil {
				entry.conn = newConn
				entry.lastUsed = now
				entry.createdAt = now
				entry.failureCount = 0
				entry.lastFailure = time.Time{}
				entry.healthChecked = now
			}
		}
	}
}

// closeAllConnections closes all connections in the pool
func (cp *ConnectionPool) closeAllConnections() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	for _, entry := range cp.connections {
		if entry.conn != nil {
			entry.conn.Close()
		}
	}
	cp.connections = nil
	cp.closed = true
}

// isConnectionHealthy validates the connection state and health
func isConnectionHealthy(conn *grpc.ClientConn) bool {
	if conn == nil {
		return false
	}
	switch conn.GetState() {
	case connectivity.Ready, connectivity.Idle, connectivity.Connecting:
		return true
	default:
		return false
	}
}

// helpers (minimal)
func recordConnectionFailure(entry *connectionEntry) {
	if entry == nil {
		return
	}
	entry.failureCount++
	entry.lastFailure = time.Now()
}

func recordConnectionSuccess(entry *connectionEntry) {
	if entry == nil {
		return
	}
	if entry.failureCount > 0 {
		entry.failureCount = 0
		entry.lastFailure = time.Time{}
	}
}

func isConnectionExcessivelyFailing(entry *connectionEntry, maxFailures int, failureWindow time.Duration) bool {
	if entry == nil {
		return true
	}
	if entry.failureCount >= maxFailures {
		if time.Since(entry.lastFailure) <= failureWindow {
			return true
		}
		entry.failureCount = 0
		entry.lastFailure = time.Time{}
	}
	return false
}
