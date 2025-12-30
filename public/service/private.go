package service

import (
	"github.com/warpstreamlabs/bento/internal/component/interop/private"
	"github.com/warpstreamlabs/bento/internal/message"
)

func (m *Message) XUnwrap() private.Internal[*message.Part] {
	return private.ToInternal(m.part)
}

func (b MessageBatch) XUnwrap() private.Internal[message.Batch] {
	ibatch := make(message.Batch, len(b))
	for i, msg := range b {
		ibatch[i] = msg.part
	}

	return private.ToInternal(ibatch)
}
