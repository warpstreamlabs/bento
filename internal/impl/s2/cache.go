package s2

import (
	"context"
	"encoding/base64"
	"encoding/binary"

	"github.com/warpstreamlabs/bento/public/service"
)

type bentoSeqNumCache struct {
	Resources *service.Resources
	Label     string
}

func streamCacheKey(stream string) string {
	return base64.URLEncoding.EncodeToString([]byte(stream))
}

func (b *bentoSeqNumCache) Get(ctx context.Context, stream string) (uint64, error) {
	var (
		seqNum uint64
		err    error
	)

	if aErr := b.Resources.AccessCache(ctx, b.Label, func(c service.Cache) {
		var seqNumBytes []byte
		seqNumBytes, err = c.Get(ctx, streamCacheKey(stream))
		if err != nil {
			return
		}

		seqNum = binary.BigEndian.Uint64(seqNumBytes)
	}); aErr != nil {
		return 0, aErr
	}

	return seqNum, err
}

func (b *bentoSeqNumCache) Set(ctx context.Context, stream string, seqNum uint64) error {
	var err error

	if aErr := b.Resources.AccessCache(ctx, b.Label, func(c service.Cache) {
		seqNumBytes := binary.BigEndian.AppendUint64(make([]byte, 0, 8), seqNum)

		err = c.Set(ctx, streamCacheKey(stream), seqNumBytes, nil)
	}); aErr != nil {
		return aErr
	}

	return err
}
