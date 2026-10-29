package nsq

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noopDelegate struct{}

func (noopDelegate) OnFinish(*nsq.Message)                       {}
func (noopDelegate) OnRequeue(*nsq.Message, time.Duration, bool) {}
func (noopDelegate) OnTouch(*nsq.Message)                        {}

func TestNSQReaderAckReleasesMessages(t *testing.T) {
	n := &nsqReader{
		unAckMsgs:        map[nsq.MessageID]*nsq.Message{},
		internalMessages: make(chan *nsq.Message),
		interruptChan:    make(chan struct{}),
	}

	const count = 100
	go func() {
		for i := range count {
			var id nsq.MessageID
			copy(id[:], strconv.Itoa(i))
			m := nsq.NewMessage(id, []byte("hello"))
			m.Delegate = noopDelegate{}
			n.internalMessages <- m
		}
	}()

	ctx := context.Background()
	for i := range count {
		_, ackFn, err := n.Read(ctx)
		require.NoError(t, err)

		var res error
		if i%2 == 0 {
			res = assert.AnError
		}
		require.NoError(t, ackFn(ctx, res))
	}

	n.unAckMut.Lock()
	defer n.unAckMut.Unlock()
	assert.Empty(t, n.unAckMsgs)
}
