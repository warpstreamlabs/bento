package component

import (
	"errors"
	"fmt"
	"time"
)

// ErrNotUnwrapped is returned in cases where a component was meant to be
// unwrapped either from the public packages or to the public packages but for
// some reason this did not happen. Unwrapping should only occur in times when
// it's guaranteed to succeed, so this error indicates that an assumption was
// incorrect during the migration of certain components which will need to be
// immediately addressed by maintainers.
var ErrNotUnwrapped = errors.New("something has gone wrong during the registering of this component, please open an issue https://github.com/warpstreamlabs/bento/issues/new to let us know")

type errInvalidType struct {
	typeStr string
	tried   string
}

func (e *errInvalidType) Error() string {
	return fmt.Sprintf("%v type of '%v' was not recognised", e.typeStr, e.tried)
}

// ErrInvalidType creates an error that describes a component type being
// initialised with an unrecognised implementation.
func ErrInvalidType(typeStr, tried string) error {
	return &errInvalidType{
		typeStr: typeStr,
		tried:   tried,
	}
}

//------------------------------------------------------------------------------

// LabelledError is an error that could be returned by components annotated by
// their label (or path) in order to provide extra context to which specific
// component within a config is yielding it. This is particularly useful in
// situations such as ConnectionStatus aggregates where a broker yields multiple
// errors from a range of child components.
type LabelledError struct {
	Label string
	Err   error
}

// Error returns a formatted error string.
func (e *LabelledError) Error() string {
	return fmt.Sprintf("%v: %v", e.Label, e.Err)
}

// Unwrap returns the underlying error value.
func (e *LabelledError) Unwrap() error {
	return e.Err
}

//------------------------------------------------------------------------------

// Errors used throughout the codebase.
var (
	ErrTimeout    = errors.New("action timed out")
	ErrTypeClosed = errors.New("type was closed")

	ErrNotConnected = errors.New("not connected to target source or sink")

	// ErrAlreadyStarted is returned when an input or output type gets started a
	// second time.
	ErrAlreadyStarted = errors.New("type has already been started")

	ErrNoAck = errors.New("failed to receive acknowledgement")

	ErrFailedSend = errors.New("message failed to reach a target destination")
)

// ErrBackOff is an error returned that allows for a back off duration to be specified
type ErrBackOff struct {
	Err  error
	Wait time.Duration
}

// Error returns the Error string.
func (e *ErrBackOff) Error() string {
	return e.Err.Error()
}

//------------------------------------------------------------------------------

// Manager errors.
var (
	ErrInputNotFound     = errors.New("input not found")
	ErrCacheNotFound     = errors.New("cache not found")
	ErrProcessorNotFound = errors.New("processor not found")
	ErrRateLimitNotFound = errors.New("rate limit not found")
	ErrOutputNotFound    = errors.New("output not found")
	ErrKeyAlreadyExists  = errors.New("key already exists")
	ErrKeyNotFound       = errors.New("key does not exist")
	ErrPipeNotFound      = errors.New("pipe was not found")
)

//------------------------------------------------------------------------------

// Buffer errors.
var (
	ErrMessageTooLarge = errors.New("message body larger than buffer space")
	ErrLimitReached    = errors.New("adding message to buffer will exceed size limit")
)
