package grpc_client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/grpcreflect"

	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Common config field names
const (
	fieldAddress     = "address"
	fieldMethod      = "method"
	fieldRPCType     = "rpc_type"
	fieldRequestJSON = "request_json"
)

// createBaseConfigSpec creates the common configuration fields shared between input and output
func createBaseConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Version("1.11.0").
		Categories("Services").
		Field(service.NewStringField(fieldAddress).Default("127.0.0.1:50051")).
		Field(service.NewStringField(fieldMethod).Description("Full method name, e.g. /pkg.Service/Method")).
		// Auth
		Field(service.NewStringField("bearer_token").Secret().Optional()).
		Field(service.NewStringMapField("auth_headers").Optional()).
		// Core timing
		Field(service.NewDurationField("call_timeout").Default("0s")).
		Field(service.NewDurationField("connect_timeout").Default("0s")).
		// Reflection / proto
		Field(service.NewStringListField("proto_files").Optional()).
		Field(service.NewStringListField("include_paths").Optional()).
		// Best practices / minimal behavior
		Field(service.NewBoolField("propagate_deadlines").Default(true)).
		// Retry policy (simple)
		Field(service.NewIntField("retry_max_attempts").Default(0)).
		Field(service.NewDurationField("retry_initial_backoff").Default("1s")).
		Field(service.NewDurationField("retry_max_backoff").Default("30s")).
		Field(service.NewFloatField("retry_backoff_multiplier").Default(2.0)).
		// Logging levels
		Field(service.NewStringField("log_level_success").Default("debug")).
		Field(service.NewStringField("log_level_error").Default("debug"))

	return spec
}

// enhanceCallContext enhances the context for gRPC calls; simplified to no-op
func enhanceCallContext(ctx context.Context, _ *Config, _ func(context.Context) context.Context) context.Context {
	return ctx
}

// injectMetadataIntoContext adds auth headers to the gRPC context (simplified)
func injectMetadataIntoContext(ctx context.Context, cfg *Config) context.Context {
	if cfg == nil {
		return ctx
	}
	md := metadata.MD{}
	if len(cfg.AuthHeaders) > 0 {
		for k, v := range cfg.AuthHeaders {
			md.Set(strings.ToLower(k), v)
		}
	}
	if cfg.BearerToken != "" {
		md.Set("authorization", "Bearer "+cfg.BearerToken)
	}
	if len(md) == 0 {
		return ctx
	}
	if existing, ok := metadata.FromOutgoingContext(ctx); ok {
		merged := existing.Copy()
		for k, v := range md {
			for _, vv := range v {
				merged.Append(k, vv)
			}
		}
		return metadata.NewOutgoingContext(ctx, merged)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// Default timing configuration constants
const (
	defaultRetryBackoffInitial   = time.Second
	defaultRetryBackoffMax       = 30 * time.Second
	defaultConnectionIdleTimeout = 30 * time.Minute
	defaultSessionSweepInterval  = time.Minute
	defaultConnectionPoolSize    = 1
	defaultRetryMultiplier       = 2.0
	defaultCleanupTickerInterval = time.Minute
	defaultHealthCheckInterval   = 30 * time.Second
	defaultMaxConnectionFailures = 3
	defaultFailureWindow         = 5 * time.Minute
)

// Magic numbers for message sizes and limits
const (
	minMethodNameLength = 3 // Minimum: "/a/b"
)

// Config represents shared gRPC client configuration
type Config struct {
	Address        string
	Method         string
	RPCType        string
	RequestJSON    string
	BearerToken    string
	AuthHeaders    map[string]string
	CallTimeout    time.Duration
	ConnectTimeout time.Duration
	ProtoFiles     []string
	IncludePaths   []string

	// gRPC best practices
	RetryPolicy *RetryPolicy

	// Logging levels
	LogLevelSuccess string
	LogLevelError   string

	// Optional call observer for outcomes
	Observer CallObserver

	// Logger for shared components
	Logger *service.Logger
}

// CallObserver receives outcomes of calls
type CallObserver interface {
	RecordCall(err error)
}

// RetryPolicy defines retry behavior for gRPC calls
type RetryPolicy struct {
	MaxAttempts          int
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
	BackoffMultiplier    float64
	RetryableStatusCodes []codes.Code
}

// Note: advanced gRPC service configs are not used in minimal mode

// ParseConfigFromService extracts gRPC configuration from service config
func ParseConfigFromService(conf *service.ParsedConfig) (*Config, error) {
	cfg := &Config{}
	extractCoreConfig(conf, cfg)
	extractAuthConfig(conf, cfg)
	extractConnectionConfig(conf, cfg)
	extractBestPracticesConfig(conf, cfg)
	extractRetryPolicyConfig(conf, cfg)
	return cfg, nil
}

// extractCoreConfig extracts fundamental gRPC configuration fields
func extractCoreConfig(conf *service.ParsedConfig, cfg *Config) {
	cfg.Address, _ = conf.FieldString(fieldAddress)
	cfg.Method, _ = conf.FieldString(fieldMethod)
	cfg.RPCType, _ = conf.FieldString(fieldRPCType)
	cfg.RequestJSON, _ = conf.FieldString(fieldRequestJSON)
}

// extractAuthConfig extracts authentication configuration
func extractAuthConfig(conf *service.ParsedConfig, cfg *Config) {
	cfg.BearerToken, _ = conf.FieldString("bearer_token")
	cfg.AuthHeaders, _ = conf.FieldStringMap("auth_headers")
}

// extractConnectionConfig extracts connection-related configuration (simplified)
func extractConnectionConfig(conf *service.ParsedConfig, cfg *Config) {
	cfg.CallTimeout, _ = conf.FieldDuration("call_timeout")
	cfg.ConnectTimeout, _ = conf.FieldDuration("connect_timeout")
	cfg.ProtoFiles, _ = conf.FieldStringList("proto_files")
	cfg.IncludePaths, _ = conf.FieldStringList("include_paths")
}

// extractBestPracticesConfig extracts minimal logging config
func extractBestPracticesConfig(conf *service.ParsedConfig, cfg *Config) {
	cfg.LogLevelSuccess, _ = conf.FieldString("log_level_success")
	cfg.LogLevelError, _ = conf.FieldString("log_level_error")
}

// extractRetryPolicyConfig extracts retry policy configuration
func extractRetryPolicyConfig(conf *service.ParsedConfig, cfg *Config) {
	maxAttempts, _ := conf.FieldInt("retry_max_attempts")
	if maxAttempts <= 0 {
		return
	}
	retryInitialBackoff, _ := conf.FieldDuration("retry_initial_backoff")
	if retryInitialBackoff <= 0 {
		retryInitialBackoff = defaultRetryBackoffInitial
	}
	retryMaxBackoff, _ := conf.FieldDuration("retry_max_backoff")
	if retryMaxBackoff <= 0 {
		retryMaxBackoff = defaultRetryBackoffMax
	}
	retryMultiplier := defaultRetryMultiplier
	if multiplier, _ := conf.FieldFloat("retry_backoff_multiplier"); multiplier > 0 {
		retryMultiplier = multiplier
	}
	cfg.RetryPolicy = &RetryPolicy{
		MaxAttempts:       maxAttempts,
		InitialBackoff:    retryInitialBackoff,
		MaxBackoff:        retryMaxBackoff,
		BackoffMultiplier: retryMultiplier,
		RetryableStatusCodes: []codes.Code{
			codes.Unavailable,
			codes.ResourceExhausted,
			codes.Aborted,
			codes.DeadlineExceeded,
		},
	}
}

// createConnection creates a single gRPC connection with minimal options
func createConnection(ctx context.Context, cfg *Config) (*grpc.ClientConn, error) {
	opts, err := buildDialOptions(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build dial options: %w", err)
	}
	conn, err := grpc.NewClient(cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}
	if cfg.ConnectTimeout > 0 {
		ctxWait, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
		conn.Connect()
		for {
			st := conn.GetState()
			if st == connectivity.Ready {
				break
			}
			if !conn.WaitForStateChange(ctxWait, st) {
				_ = conn.Close()
				if err := ctxWait.Err(); err != nil {
					return nil, err
				}
				return nil, errors.New("connection not ready within connect_timeout")
			}
		}
	}
	return conn, nil
}

// headerCreds implements credentials.PerRPCCredentials with enhanced security
type headerCreds struct {
	token      string
	headers    map[string]string
	tlsEnabled bool
}

func (h headerCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	md := make(map[string]string)
	if h.token != "" {
		md["authorization"] = "Bearer " + h.token
	}
	for k, v := range h.headers {
		if err := validateHeaderValue(v); err != nil {
			continue
		}
		md[strings.ToLower(k)] = v
	}
	return md, nil
}

// validateHeaderValue validates that a header value is safe
func validateHeaderValue(value string) error {
	if len(value) > 4096 {
		return errors.New("header value is too long (maximum 4096 characters)")
	}
	for _, char := range value {
		if char < 32 && char != '\t' {
			return errors.New("header value contains control character")
		}
		if char == '\r' || char == '\n' {
			return errors.New("header value contains CRLF characters (potential header injection)")
		}
	}
	return nil
}

func (h headerCreds) RequireTransportSecurity() bool { return h.tlsEnabled }

// CircuitBreakerState represents the state of the circuit breaker
type CircuitBreakerState int

const (
	CircuitBreakerClosed   CircuitBreakerState = iota // Normal operation
	CircuitBreakerOpen                                // Failing, reject requests
	CircuitBreakerHalfOpen                            // Testing if service recovered
)

// String returns the string representation of the circuit breaker state
func (s CircuitBreakerState) String() string {
	switch s {
	case CircuitBreakerClosed:
		return "closed"
	case CircuitBreakerOpen:
		return "open"
	case CircuitBreakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern for connection management
type CircuitBreaker struct {
	state       CircuitBreakerState
	failures    int
	lastFailure time.Time
	nextAttempt time.Time
	mu          sync.RWMutex

	// Configuration
	failureThreshold int           // Number of failures before opening circuit
	resetTimeout     time.Duration // Time to wait before trying again
	halfOpenMaxReqs  int           // Max requests allowed in half-open state
	halfOpenCount    int           // Current requests in half-open state
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            CircuitBreakerClosed,
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		halfOpenMaxReqs:  3, // Allow 3 test requests in half-open state
	}
}

// CanExecute checks if a request can be executed based on circuit breaker state
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case CircuitBreakerClosed:
		return true
	case CircuitBreakerOpen:
		if time.Now().After(cb.nextAttempt) {
			cb.mu.RUnlock()
			cb.mu.Lock()
			cb.state = CircuitBreakerHalfOpen
			cb.halfOpenCount = 0
			cb.mu.Unlock()
			cb.mu.RLock()
			return true
		}
		return false
	case CircuitBreakerHalfOpen:
		return cb.halfOpenCount < cb.halfOpenMaxReqs
	default:
		return false
	}
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitBreakerHalfOpen:
		cb.halfOpenCount++
		if cb.halfOpenCount >= cb.halfOpenMaxReqs {
			// Transition back to closed state
			cb.state = CircuitBreakerClosed
			cb.failures = 0
			cb.lastFailure = time.Time{}
		}
	case CircuitBreakerClosed:
		// Reset failure count on success
		cb.failures = 0
		cb.lastFailure = time.Time{}
	}
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitBreakerHalfOpen:
		// Half-open failure, go back to open
		cb.state = CircuitBreakerOpen
		cb.nextAttempt = time.Now().Add(cb.resetTimeout)
	case CircuitBreakerClosed:
		if cb.failures >= cb.failureThreshold {
			cb.state = CircuitBreakerOpen
			cb.nextAttempt = time.Now().Add(cb.resetTimeout)
		}
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetStats returns circuit breaker statistics
func (cb *CircuitBreaker) GetStats() map[string]interface{} {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return map[string]interface{}{
		"state":             cb.state.String(),
		"failures":          cb.failures,
		"last_failure":      cb.lastFailure.Format(time.RFC3339),
		"next_attempt":      cb.nextAttempt.Format(time.RFC3339),
		"half_open_count":   cb.halfOpenCount,
		"failure_threshold": cb.failureThreshold,
		"reset_timeout":     cb.resetTimeout.String(),
	}
}

// ConnectionManager manages gRPC connections with proper lifecycle and pooling.
//
// The manager coordinates between connection pooling, automatic cleanup, and
// graceful shutdown. It runs a background goroutine that periodically checks
// for idle connections and replaces them to maintain connection freshness.
//
// Key Responsibilities:
// - Connection pool lifecycle management
// - Automatic cleanup of idle connections
// - Thread-safe access coordination
// - Graceful shutdown without resource leaks
// - Circuit breaker pattern integration
type ConnectionManager struct {
	pool           *ConnectionPool // Underlying connection pool
	circuitBreaker *CircuitBreaker // Circuit breaker for fault tolerance
	mu             sync.RWMutex    // Protects manager state during shutdown
	closed         bool            // Indicates if manager is shut down
}

// NewConnectionManager creates a new connection manager with pooling support
func NewConnectionManager(ctx context.Context, cfg *Config) (*ConnectionManager, error) {
	pool := &ConnectionPool{
		connections: make([]connectionEntry, 0, 1),
		cfg:         cfg,
		ctx:         ctx,
	}
	// Circuit breaker with fixed sensible defaults
	circuitBreaker := NewCircuitBreaker(5, 30*time.Second)
	// Startup warnings
	if cfg.Logger != nil {
		if cfg.BearerToken != "" {
			cfg.Logger.Warnf("Using bearer_token over insecure transport. Avoid sending credentials without TLS.")
		}
		for k := range cfg.AuthHeaders {
			kl := strings.ToLower(k)
			if strings.Contains(kl, "password") || strings.Contains(kl, "secret") || strings.Contains(kl, "token") || strings.Contains(kl, "key") {
				cfg.Logger.Warnf("Auth header key '%s' may contain sensitive data. Ensure secure transport.", k)
			}
		}
	}
	for i := 0; i < 1; i++ {
		conn, err := createConnection(ctx, cfg)
		if err != nil {
			pool.closeAllConnections()
			return nil, fmt.Errorf("failed to create connection %d: %w", i, err)
		}
		now := time.Now()
		pool.connections = append(pool.connections, connectionEntry{conn: conn, lastUsed: now, createdAt: now, healthChecked: now})
	}
	cm := &ConnectionManager{pool: pool, circuitBreaker: circuitBreaker}
	go cm.cleanupIdleConnections()
	return cm, nil
}

// createConnection unchanged except uses cfg.ConnectTimeout

// GetConnection returns an available gRPC connection from the pool (thread-safe)
func (cm *ConnectionManager) GetConnection() (*grpc.ClientConn, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.closed {
		return nil, errors.New("connection manager is closed")
	}

	// Check circuit breaker before attempting to get connection
	if !cm.circuitBreaker.CanExecute() {
		return nil, errors.New("circuit breaker is open - service unavailable")
	}

	conn, err := cm.pool.getConnection()
	if err != nil {
		// Record failure in circuit breaker
		cm.circuitBreaker.RecordFailure()
		return nil, err
	}

	// Record success in circuit breaker
	cm.circuitBreaker.RecordSuccess()
	return conn, nil
}

// ValidateConnection checks if a specific connection is healthy and ready for use
func (cm *ConnectionManager) ValidateConnection(conn *grpc.ClientConn) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.closed || conn == nil {
		return false
	}

	return isConnectionHealthy(conn)
}

// cleanupIdleConnections uses fixed interval
func (cm *ConnectionManager) cleanupIdleConnections() {
	interval := defaultCleanupTickerInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		cm.mu.RLock()
		if cm.closed {
			cm.mu.RUnlock()
			return
		}
		cm.mu.RUnlock()
		cm.pool.cleanupIdle()
	}
}

// Close closes the connection manager and all connections (thread-safe)
func (cm *ConnectionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed {
		return nil
	}

	cm.closed = true

	if cm.pool != nil {
		cm.pool.closeAllConnections()
	}

	return nil
}

// GetCircuitBreakerState returns the current state of the circuit breaker
func (cm *ConnectionManager) GetCircuitBreakerState() CircuitBreakerState {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.closed || cm.circuitBreaker == nil {
		return CircuitBreakerClosed
	}

	return cm.circuitBreaker.GetState()
}

// buildDialOptions simplified: insecure creds, per-RPC creds, deadlines, logging, retry
func buildDialOptions(ctx context.Context, cfg *Config) ([]grpc.DialOption, error) {
	var opts []grpc.DialOption
	transportCreds, err := buildTransportCredentials(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build transport credentials: %w", err)
	}
	opts = append(opts, grpc.WithTransportCredentials(transportCreds))
	// Per-RPC credentials
	if cfg.BearerToken != "" || len(cfg.AuthHeaders) > 0 {
		opts = append(opts, grpc.WithPerRPCCredentials(headerCreds{token: cfg.BearerToken, headers: cfg.AuthHeaders, tlsEnabled: false}))
	}
	// Interceptors: deadlines, logging, retry
	unaryInterceptors, streamInterceptors := buildInterceptors(cfg)
	if len(unaryInterceptors) > 0 {
		opts = append(opts, grpc.WithChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(streamInterceptors) > 0 {
		opts = append(opts, grpc.WithChainStreamInterceptor(streamInterceptors...))
	}
	return opts, nil
}

// buildTransportCredentials returns insecure credentials (TLS disabled)
func buildTransportCredentials(cfg *Config) (credentials.TransportCredentials, error) {
	return insecure.NewCredentials(), nil
}

// buildDefaultCallOptions left unchanged (not used)

// buildInterceptors creates gRPC interceptors for observability and best practices
func buildInterceptors(cfg *Config) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	var unaryInterceptors []grpc.UnaryClientInterceptor
	var streamInterceptors []grpc.StreamClientInterceptor
	// Always propagate deadlines
	unaryInterceptors = append(unaryInterceptors, deadlineUnaryInterceptor)
	streamInterceptors = append(streamInterceptors, deadlineStreamInterceptor)
	// Logging
	if cfg.Logger != nil {
		log := cfg.Logger
		unaryInterceptors = append(unaryInterceptors, func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			start := time.Now()
			err := invoker(ctx, method, req, reply, cc, opts...)
			dur := time.Since(start)
			st, _ := status.FromError(err)
			if err != nil {
				logAtLevel(log, cfg.LogLevelError, "grpc unary call failed", method, st.Code().String(), dur)
				if cfg.Observer != nil {
					cfg.Observer.RecordCall(err)
				}
			} else {
				logAtLevel(log, cfg.LogLevelSuccess, "grpc unary call ok", method, "OK", dur)
				if cfg.Observer != nil {
					cfg.Observer.RecordCall(nil)
				}
			}
			return err
		})
		streamInterceptors = append(streamInterceptors, func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			start := time.Now()
			cs, err := streamer(ctx, desc, cc, method, opts...)
			dur := time.Since(start)
			st, _ := status.FromError(err)
			if err != nil {
				logAtLevel(log, cfg.LogLevelError, "grpc stream open failed", method, st.Code().String(), dur)
				if cfg.Observer != nil {
					cfg.Observer.RecordCall(err)
				}
			} else {
				logAtLevel(log, cfg.LogLevelSuccess, "grpc stream opened", method, "OK", dur)
				if cfg.Observer != nil {
					cfg.Observer.RecordCall(nil)
				}
			}
			return cs, err
		})
	}
	// Retry
	if cfg.RetryPolicy != nil {
		unaryInterceptors = append(unaryInterceptors, retryUnaryInterceptor(cfg.RetryPolicy))
	}
	return unaryInterceptors, streamInterceptors
}

// deadlineUnaryInterceptor propagates deadlines from context with enhanced handling
func deadlineUnaryInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// Enhanced context deadline handling
	ctx = enhanceContextWithDeadlines(ctx)

	// Add method-specific timeout if none exists
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		// Apply default timeout for unary calls
		defaultTimeout := 30 * time.Second
		newCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
		ctx = newCtx
	}

	// Ensure context is properly canceled on completion
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return invoker(ctx, method, req, reply, cc, opts...)
}

// deadlineStreamInterceptor propagates deadlines for streaming calls with enhanced handling
func deadlineStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// Enhanced context deadline handling for streams
	ctx = enhanceContextWithDeadlines(ctx)

	// Add method-specific timeout for streaming calls if none exists
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		// Apply longer default timeout for streaming calls
		defaultTimeout := 5 * time.Minute
		newCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
		ctx = newCtx
	}

	return streamer(ctx, desc, cc, method, opts...)
}

// enhanceContextWithDeadlines enhances context with proper deadline handling and propagation
func enhanceContextWithDeadlines(ctx context.Context) context.Context {
	// Check if we already have a deadline
	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		// Calculate remaining time
		remaining := time.Until(deadline)

		// If deadline is too close, leave as-is (avoid creating new timers)
		minRemainingTime := 100 * time.Millisecond
		if remaining < minRemainingTime {
			return ctx
		}

		// Deadline is reasonable, use as-is
		return ctx
	}

	// No deadline exists, return original context
	return ctx
}

// retryUnaryInterceptor implements client-side retry logic
func retryUnaryInterceptor(retryPolicy *RetryPolicy) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var lastErr error
		backoff := retryPolicy.InitialBackoff

		for attempt := 0; attempt < retryPolicy.MaxAttempts; attempt++ {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil // Success
			}

			lastErr = err

			// Check if error is retryable
			if !isRetryableError(err, retryPolicy.RetryableStatusCodes) {
				return err
			}

			// Don't retry on last attempt
			if attempt == retryPolicy.MaxAttempts-1 {
				break
			}

			// Sleep with backoff
			select {
			case <-time.After(backoff):
				backoff = time.Duration(float64(backoff) * retryPolicy.BackoffMultiplier)
				if backoff > retryPolicy.MaxBackoff {
					backoff = retryPolicy.MaxBackoff
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return lastErr
	}
}

// isRetryableError determines if an error should be retried
func isRetryableError(err error, retryableCodes []codes.Code) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	for _, code := range retryableCodes {
		if st.Code() == code {
			return true
		}
	}

	return false
}

// MethodResolver handles method resolution with caching and performance optimizations
type MethodResolver struct {
	cache sync.Map // string -> *methodCacheEntry
}

// methodCacheEntry holds both the method descriptor and message pools
type methodCacheEntry struct {
	method *desc.MethodDescriptor
}

// NewMethodResolver creates a new method resolver
func NewMethodResolver() *MethodResolver {
	return &MethodResolver{}
}

// ResolveMethod resolves a method using reflection or proto files with enhanced caching
func (mr *MethodResolver) ResolveMethod(ctx context.Context, conn *grpc.ClientConn, cfg *Config) (*desc.MethodDescriptor, error) {
	// Check cache first
	key := mr.cacheKey(cfg)
	if cached, ok := mr.cache.Load(key); ok {
		entry := cached.(*methodCacheEntry)
		return entry.method, nil
	}
	var method *desc.MethodDescriptor
	var err error
	if len(cfg.ProtoFiles) > 0 {
		method, err = mr.resolveFromProtoFiles(cfg.Method, cfg.ProtoFiles, cfg.IncludePaths)
	} else {
		method, err = mr.resolveFromReflection(ctx, conn, cfg.Method)
	}
	if err != nil {
		return nil, err
	}
	entry := &methodCacheEntry{method: method}
	mr.cache.Store(key, entry)
	mr.cache.Store(method.GetFullyQualifiedName(), entry)
	return method, nil
}

// GetMessagePools removed in minimal mode

// resolveFromReflection resolves method using gRPC reflection
func (mr *MethodResolver) resolveFromReflection(ctx context.Context, conn *grpc.ClientConn, methodName string) (*desc.MethodDescriptor, error) {
	rc := grpcreflect.NewClientAuto(ctx, conn)
	defer rc.Reset()

	svcName, mName, err := parseMethodName(methodName)
	if err != nil {
		return nil, err
	}

	svc, err := rc.ResolveService(svcName)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve service %s: %w", svcName, err)
	}

	method := svc.FindMethodByName(mName)
	if method == nil {
		return nil, fmt.Errorf("method not found: %s", methodName)
	}

	return method, nil
}

// resolveFromProtoFiles resolves method from proto files
func (mr *MethodResolver) resolveFromProtoFiles(methodName string, protoFiles, includePaths []string) (*desc.MethodDescriptor, error) {
	var parser protoparse.Parser
	if len(includePaths) > 0 {
		parser.ImportPaths = includePaths
	}

	fds, err := parser.ParseFiles(protoFiles...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto files: %w", err)
	}

	svcName, mName, err := parseMethodName(methodName)
	if err != nil {
		return nil, err
	}

	for _, fd := range fds {
		for _, svc := range fd.GetServices() {
			if svc.GetFullyQualifiedName() == svcName || svc.GetName() == svcName {
				if method := svc.FindMethodByName(mName); method != nil {
					return method, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("method not found in provided proto files: %s", methodName)
}

// parseMethodName parses a method name like "/pkg.Service/Method" into service and method names
// Optimized to avoid repeated string operations and memory allocations
func parseMethodName(full string) (string, string, error) {
	// Fast path: check minimum length and format
	if len(full) < minMethodNameLength { // Minimum: "/a/b"
		return "", "", fmt.Errorf("invalid method format: %s (too short)", full)
	}

	// Remove leading slash efficiently
	start := 0
	if full[0] == '/' {
		start = 1
	}

	// Find the last slash to separate service and method (single pass)
	lastSlash := -1
	for i := len(full) - 1; i >= start; i-- {
		if full[i] == '/' {
			lastSlash = i
			break
		}
	}

	if lastSlash == -1 || lastSlash == start {
		return "", "", fmt.Errorf("invalid method format: %s (expected format: /service/method)", full)
	}

	serviceName := full[start:lastSlash]
	methodName := full[lastSlash+1:]

	// Validate non-empty (avoid string comparison)
	if len(serviceName) == 0 || len(methodName) == 0 {
		return "", "", fmt.Errorf("invalid method format: %s (service and method names cannot be empty)", full)
	}

	return serviceName, methodName, nil
}

// RetryConfig holds retry configuration
type RetryConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxRetries     int
}

// DefaultRetryConfig returns sensible retry defaults
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		InitialBackoff: defaultRetryBackoffInitial,
		MaxBackoff:     defaultRetryBackoffMax,
		MaxRetries:     5,
	}
}

// WithContextRetry performs an operation with exponential backoff retry and context awareness.
//
// Retry Strategy:
// - Implements exponential backoff with configurable initial delay and multiplier
// - Respects maximum backoff duration to prevent excessive wait times
// - Honors context cancellation at any point during retry attempts
// - Uses intelligent error classification for retry decisions
//
// Context Handling:
// - Checks for context cancellation before each retry attempt
// - Cancellation during backoff sleep immediately returns context error
// - Preserves last operation error when context is cancelled
//
// Error Handling:
// - Returns immediately on successful operation (nil error)
// - Accumulates the last error from failed attempts
// - Provides comprehensive error context including attempt count
// - Uses error classification to determine retry eligibility
//
// Usage Pattern:
// This is typically used for transient failures in gRPC operations where
// temporary network issues or service unavailability should be retried.
func WithContextRetry(ctx context.Context, cfg RetryConfig, operation func() error) error {
	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Check context cancellation before each attempt
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("context cancelled, last error: %w", lastErr)
			}
			return ctx.Err()
		default:
		}

		if err := operation(); err != nil {
			lastErr = err

			// Use error classification to determine if we should retry
			classifiedErr := classifyGrpcError("", err)
			if classifiedErr != nil && !classifiedErr.IsRetryable() {
				// Don't retry non-retryable errors
				return fmt.Errorf("non-retryable error on attempt %d: %w", attempt+1, err)
			}

			// Don't sleep after the last attempt
			if attempt == cfg.MaxRetries {
				break
			}

			// Sleep with backoff, but respect context cancellation
			select {
			case <-time.After(backoff):
				// Exponential backoff with jitter
				backoff = time.Duration(float64(backoff) * defaultRetryMultiplier)

				// Add small jitter to prevent thundering herd (±10%)
				jitterFactor := 0.1 * (2*rand.Float64() - 1) // Random value between -0.1 and 0.1
				jitter := time.Duration(float64(backoff) * jitterFactor)
				backoff += jitter

				if backoff > cfg.MaxBackoff {
					backoff = cfg.MaxBackoff
				}
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during backoff, last error: %w", lastErr)
			}
		} else {
			return nil // Success
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", cfg.MaxRetries+1, lastErr)
}

// ErrorType represents the classification of different types of errors
type ErrorType int

const (
	ErrorTypeUnknown ErrorType = iota
	ErrorTypeConnection
	ErrorTypeTimeout
	ErrorTypeAuthentication
	ErrorTypeAuthorization
	ErrorTypeRateLimit
	ErrorTypeResourceExhausted
	ErrorTypeUnavailable
	ErrorTypeInternal
	ErrorTypeInvalidArgument
	ErrorTypeNotFound
	ErrorTypeAlreadyExists
	ErrorTypeFailedPrecondition
	ErrorTypeAborted
	ErrorTypeOutOfRange
	ErrorTypeUnimplemented
	ErrorTypeDataLoss
	ErrorTypeCancelled
	ErrorTypeDeadlineExceeded
)

// String returns the string representation of the error type
func (et ErrorType) String() string {
	switch et {
	case ErrorTypeConnection:
		return "connection"
	case ErrorTypeTimeout:
		return "timeout"
	case ErrorTypeAuthentication:
		return "authentication"
	case ErrorTypeAuthorization:
		return "authorization"
	case ErrorTypeRateLimit:
		return "rate_limit"
	case ErrorTypeResourceExhausted:
		return "resource_exhausted"
	case ErrorTypeUnavailable:
		return "unavailable"
	case ErrorTypeInternal:
		return "internal"
	case ErrorTypeInvalidArgument:
		return "invalid_argument"
	case ErrorTypeNotFound:
		return "not_found"
	case ErrorTypeAlreadyExists:
		return "already_exists"
	case ErrorTypeFailedPrecondition:
		return "failed_precondition"
	case ErrorTypeAborted:
		return "aborted"
	case ErrorTypeOutOfRange:
		return "out_of_range"
	case ErrorTypeUnimplemented:
		return "unimplemented"
	case ErrorTypeDataLoss:
		return "data_loss"
	case ErrorTypeCancelled:
		return "cancelled"
	case ErrorTypeDeadlineExceeded:
		return "deadline_exceeded"
	default:
		return "unknown"
	}
}

// GrpcError represents a classified gRPC error with additional context
type GrpcError struct {
	Type        ErrorType
	Code        codes.Code
	Message     string
	Method      string
	Details     []string
	Retryable   bool
	OriginalErr error
}

// Error implements the error interface
func (e *GrpcError) Error() string {
	return fmt.Sprintf("%s (%s): %s", e.Type.String(), e.Method, e.Message)
}

// Unwrap returns the original error for error wrapping
func (e *GrpcError) Unwrap() error {
	return e.OriginalErr
}

// IsRetryable returns whether this error should be retried
func (e *GrpcError) IsRetryable() bool {
	return e.Retryable
}

// classifyGrpcError analyzes a gRPC error and returns a classified GrpcError
func classifyGrpcError(method string, err error) *GrpcError {
	if err == nil {
		return nil
	}

	grpcErr := &GrpcError{
		Method:      method,
		OriginalErr: err,
	}

	// Check for gRPC status errors
	if st, ok := status.FromError(err); ok {
		grpcErr.Code = st.Code()
		grpcErr.Message = st.Message()

		// Collect details if any
		for _, d := range st.Details() {
			if pm, ok := d.(proto.Message); ok {
				b, _ := protojson.Marshal(pm)
				grpcErr.Details = append(grpcErr.Details, string(b))
			}
		}

		// Classify based on gRPC status code
		switch st.Code() {
		case codes.Canceled:
			grpcErr.Type = ErrorTypeCancelled
			grpcErr.Retryable = false // Don't retry cancelled operations
		case codes.Unknown:
			grpcErr.Type = ErrorTypeUnknown
			grpcErr.Retryable = true // May be transient
		case codes.InvalidArgument:
			grpcErr.Type = ErrorTypeInvalidArgument
			grpcErr.Retryable = false // Client error, don't retry
		case codes.DeadlineExceeded:
			grpcErr.Type = ErrorTypeDeadlineExceeded
			grpcErr.Retryable = true // Network timeout, retry possible
		case codes.NotFound:
			grpcErr.Type = ErrorTypeNotFound
			grpcErr.Retryable = false // Resource doesn't exist
		case codes.AlreadyExists:
			grpcErr.Type = ErrorTypeAlreadyExists
			grpcErr.Retryable = false // Resource conflict
		case codes.PermissionDenied:
			grpcErr.Type = ErrorTypeAuthorization
			grpcErr.Retryable = false // Authorization failure
		case codes.ResourceExhausted:
			grpcErr.Type = ErrorTypeResourceExhausted
			grpcErr.Retryable = true // May be temporary resource exhaustion
		case codes.FailedPrecondition:
			grpcErr.Type = ErrorTypeFailedPrecondition
			grpcErr.Retryable = false // Preconditions not met
		case codes.Aborted:
			grpcErr.Type = ErrorTypeAborted
			grpcErr.Retryable = true // May be transient
		case codes.OutOfRange:
			grpcErr.Type = ErrorTypeOutOfRange
			grpcErr.Retryable = false // Invalid range
		case codes.Unimplemented:
			grpcErr.Type = ErrorTypeUnimplemented
			grpcErr.Retryable = false // Method not implemented
		case codes.Internal:
			grpcErr.Type = ErrorTypeInternal
			grpcErr.Retryable = true // Server internal error, may be transient
		case codes.Unavailable:
			grpcErr.Type = ErrorTypeUnavailable
			grpcErr.Retryable = true // Service unavailable, definitely retry
		case codes.DataLoss:
			grpcErr.Type = ErrorTypeDataLoss
			grpcErr.Retryable = false // Data corruption, don't retry
		case codes.Unauthenticated:
			grpcErr.Type = ErrorTypeAuthentication
			grpcErr.Retryable = false // Authentication failure
		default:
			grpcErr.Type = ErrorTypeUnknown
			grpcErr.Retryable = false // Conservative default
		}

		return grpcErr
	}

	// Handle non-gRPC errors (connection errors, etc.)
	errStr := err.Error()
	grpcErr.Message = errStr

	// Classify based on error message patterns
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "dial") ||
		strings.Contains(errStr, "network") || strings.Contains(errStr, "timeout") {
		grpcErr.Type = ErrorTypeConnection
		grpcErr.Retryable = true
	} else if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline") {
		grpcErr.Type = ErrorTypeTimeout
		grpcErr.Retryable = true
	} else {
		grpcErr.Type = ErrorTypeUnknown
		grpcErr.Retryable = false
	}

	return grpcErr
}

// formatGrpcError returns a richer error string including status code and details
func formatGrpcError(prefix, method string, err error) error {
	classifiedErr := classifyGrpcError(method, err)
	if classifiedErr != nil {
		return classifiedErr
	}
	return fmt.Errorf("%s (%s): %w", prefix, method, err)
}

// logAtLevel emits a structured message at a given level
func logAtLevel(log *service.Logger, level string, msg string, method string, code string, dur time.Duration) {
	entry := log.With("method", method, "code", code, "duration", dur.String())
	switch strings.ToLower(level) {
	case "warn", "warning":
		entry.Warnf(msg)
	case "info":
		entry.Infof(msg)
	default:
		entry.Debugf(msg)
	}
}

func (mr *MethodResolver) cacheKey(cfg *Config) string {
	if len(cfg.ProtoFiles) == 0 {
		return cfg.Method + "|reflect"
	}
	return cfg.Method + "|" + strings.Join(cfg.ProtoFiles, ",") + "|" + strings.Join(cfg.IncludePaths, ",")
}
