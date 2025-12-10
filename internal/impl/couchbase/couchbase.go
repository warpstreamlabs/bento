package couchbase

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/gocb/v2"
)

func valueFromOp(op gocb.BulkOp) (out any, cas gocb.Cas, err error) {
	switch o := op.(type) {
	case *gocb.GetOp:
		if o.Err != nil {
			return nil, gocb.Cas(0), o.Err
		}
		err := o.Result.Content(&out)

		return out, o.Result.Cas(), err
	case *gocb.InsertOp:
		if o.Result != nil {
			return nil, o.Result.Cas(), o.Err
		}
		return nil, gocb.Cas(0), o.Err
	case *gocb.RemoveOp:
		if o.Result != nil {
			return nil, o.Result.Cas(), o.Err
		}
		return nil, gocb.Cas(0), o.Err
	case *gocb.ReplaceOp:
		if o.Result != nil {
			return nil, o.Result.Cas(), o.Err
		}
		return nil, gocb.Cas(0), o.Err
	case *gocb.UpsertOp:
		if o.Result != nil {
			return nil, o.Result.Cas(), o.Err
		}
		return nil, gocb.Cas(0), o.Err
	case *gocb.IncrementOp:
		if o.Result != nil {
			return o.Result.Content(), o.Result.Cas(), o.Err
		}
		return nil, gocb.Cas(0), o.Err
	case *gocb.DecrementOp:
		if o.Result != nil {
			return o.Result.Content(), o.Result.Cas(), o.Err
		}
		return nil, gocb.Cas(0), o.Err
	}

	return nil, gocb.Cas(0), errors.New("type not supported")
}

func get(key string, _ []byte, _ gocb.Cas, _ *time.Duration) (gocb.BulkOp, error) {
	return &gocb.GetOp{
		ID: key,
	}, nil
}

func insert(key string, data []byte, _ gocb.Cas, ttl *time.Duration) (gocb.BulkOp, error) {
	op := &gocb.InsertOp{
		ID:    key,
		Value: data,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op, nil
}

func remove(key string, _ []byte, cas gocb.Cas, _ *time.Duration) (gocb.BulkOp, error) {
	return &gocb.RemoveOp{
		ID:  key,
		Cas: cas,
	}, nil
}

func replace(key string, data []byte, cas gocb.Cas, ttl *time.Duration) (gocb.BulkOp, error) {
	op := &gocb.ReplaceOp{
		ID:    key,
		Value: data,
		Cas:   cas,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op, nil
}

func upsert(key string, data []byte, cas gocb.Cas, ttl *time.Duration) (gocb.BulkOp, error) {
	op := &gocb.UpsertOp{
		ID:    key,
		Value: data,
		Cas:   cas,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op, nil
}

func increment(key string, data []byte, _ gocb.Cas, ttl *time.Duration) (gocb.BulkOp, error) {
	delta, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse increment value (require integer value): %s", err)
	}

	op := &gocb.IncrementOp{
		ID:      key,
		Delta:   delta,
		Initial: delta, // Default initial to delta
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op, nil
}

func decrement(key string, data []byte, _ gocb.Cas, ttl *time.Duration) (gocb.BulkOp, error) {
	delta, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse decrement value (require integer value): %s", err)
	}

	op := &gocb.DecrementOp{
		ID:      key,
		Delta:   delta,
		Initial: -delta, // Default initial to -delta
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op, nil
}
