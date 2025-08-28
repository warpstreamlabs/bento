package grpc_client

import (
	"context"
	"testing"
)

func TestSessionManager_SetGetRemove(t *testing.T) {
	sm := NewSessionManager(0, 0, nil)
	defer sm.Close()

	if sm.GetSession("k") != nil {
		t.Fatal("expected no session initially")
	}

	_, cancel := context.WithCancel(context.Background())
	s := NewStreamSession(&struct{}{}, cancel)
	sm.SetSession("k", s)

	if sm.GetSession("k") == nil {
		t.Fatal("expected session present after SetSession")
	}

	sm.RemoveSession("k")
	if sm.GetSession("k") != nil {
		t.Fatal("expected session removed after RemoveSession")
	}
}
