package component

// ConnectionStatus represents the current connection status of a given
// component.
type ConnectionStatus struct {
	Label     string
	Path      []string
	Connected bool
	Err       error
}

type ConnectionStatuses []*ConnectionStatus

func (s ConnectionStatuses) AllActive() bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if !c.Connected {
			return false
		}
	}
	return true
}

// ConnectionFailing returns a ConnectionStatus representing a component
// connection where we are attempting to connect to the service but are
// currently unable due to the provided error.
func ConnectionFailing(o Observability, err error) *ConnectionStatus {
	return &ConnectionStatus{
		Label:     o.Label(),
		Path:      o.Path(),
		Connected: false,
		Err:       err,
	}
}

// ConnectionActive returns a ConnectionStatus representing a component
// connection where we have an active connection.
func ConnectionActive(o Observability) *ConnectionStatus {
	return &ConnectionStatus{
		Label:     o.Label(),
		Path:      o.Path(),
		Connected: true,
	}
}

// ConnectionPending returns a ConnectionStatus representing a component that
// has not yet attempted to establish its connection.
func ConnectionPending(o Observability) *ConnectionStatus {
	return &ConnectionStatus{
		Label:     o.Label(),
		Path:      o.Path(),
		Connected: false,
	}
}

// ConnectionClosed returns a ConnectionStatus representing a component that has
// intentionally closed its connection.
func ConnectionClosed(o Observability) *ConnectionStatus {
	return &ConnectionStatus{
		Label:     o.Label(),
		Path:      o.Path(),
		Connected: false,
	}
}
