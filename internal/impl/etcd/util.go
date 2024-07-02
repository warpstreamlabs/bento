package etcd

import (
	"unicode/utf8"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func etcdEventsToMap(events []*clientv3.Event) []map[string]any {
	eventsMap := make([]map[string]any, len(events))

	for i, e := range events {
		event := map[string]any{
			"key":             e.Kv.Key,
			"value":           e.Kv.Value,
			"type":            mvccpb.Event_EventType_name[int32(e.Type)],
			"version":         e.Kv.Version,
			"mod_revision":    e.Kv.ModRevision,
			"create_revision": e.Kv.CreateRevision,
			"lease":           e.Kv.Lease,
		}

		if utf8.Valid(e.Kv.Key) {
			event["key"] = string(e.Kv.Key)
		}

		if utf8.Valid(e.Kv.Value) {
			event["value"] = string(e.Kv.Value)
		}

		eventsMap[i] = event
	}

	return eventsMap
}
