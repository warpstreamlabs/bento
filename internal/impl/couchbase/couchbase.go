package couchbase

import (
	"errors"
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
	}

	return nil, gocb.Cas(0), errors.New("type not supported")
}

func get(key string, _ []byte, _ gocb.Cas, _ *time.Duration) gocb.BulkOp {
	return &gocb.GetOp{
		ID: key,
	}
}

func insert(key string, data []byte, _ gocb.Cas, ttl *time.Duration) gocb.BulkOp {
	op := &gocb.InsertOp{
		ID:    key,
		Value: data,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op
}

func remove(key string, _ []byte, cas gocb.Cas, _ *time.Duration) gocb.BulkOp {
	return &gocb.RemoveOp{
		ID:  key,
		Cas: cas,
	}
}

func replace(key string, data []byte, cas gocb.Cas, ttl *time.Duration) gocb.BulkOp {
	op := &gocb.ReplaceOp{
		ID:    key,
		Value: data,
		Cas:   cas,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op
}

func upsert(key string, data []byte, cas gocb.Cas, ttl *time.Duration) gocb.BulkOp {
	op := &gocb.UpsertOp{
		ID:    key,
		Value: data,
		Cas:   cas,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op
}
