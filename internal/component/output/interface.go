package output

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/message"
)

// Sync is a common interface implemented by outputs and provides synchronous
// based writing APIs.
type Sync interface {
	// WriteTransaction attempts to write a transaction to an output.
	WriteTransaction(context.Context, message.Transaction) error

	// ConnectionStatus returns the current status of the given component
	// connection. The result is a slice in order to accommodate higher order
	// components that wrap several others.
	ConnectionStatus() component.ConnectionStatuses

	// TriggerStopConsuming instructs the output to start shutting down
	// resources once all pending messages are delivered and acknowledged.
	TriggerStopConsuming()

	// TriggerCloseNow triggers the shut down of this component but should not
	// block the calling goroutine.
	TriggerCloseNow()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(ctx context.Context) error
}

// Streamed is a common interface implemented by outputs and provides channel
// based streaming APIs.
type Streamed interface {
	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan message.Transaction) error

	// ConnectionStatus returns the current status of the given component
	// connection. The result is a slice in order to accommodate higher order
	// components that wrap several others.
	ConnectionStatus() component.ConnectionStatuses

	// TriggerCloseNow triggers the shut down of this component but should not
	// block the calling goroutine.
	TriggerCloseNow()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(ctx context.Context) error
}
