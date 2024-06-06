package io

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/v1/internal/component/output"
	bmock "github.com/warpstreamlabs/bento/v1/internal/manager/mock"
	"github.com/warpstreamlabs/bento/v1/internal/message"

	_ "github.com/warpstreamlabs/bento/v1/internal/impl/pure"
)

func TestDynamicOutputAPI(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	gMux := mux.NewRouter()

	mgr := bmock.NewManager()
	mgr.OnRegisterEndpoint = func(path string, h http.HandlerFunc) {
		gMux.HandleFunc(path, h)
	}

	conf := output.NewConfig()
	conf.Type = "dynamic"

	o, err := mgr.NewOutput(conf)
	require.NoError(t, err)

	tChan := make(chan message.Transaction)
	resChan := make(chan error, 1)
	require.NoError(t, o.Consume(tChan))

	req := httptest.NewRequest(http.MethodGet, "/outputs", http.NoBody)
	res := httptest.NewRecorder()
	gMux.ServeHTTP(res, req)

	assert.Equal(t, 200, res.Code)
	assert.Equal(t, `{}`, res.Body.String())

	fooConf := `drop: {}`
	req = httptest.NewRequest("POST", "/outputs/foo", bytes.NewBufferString(fooConf))
	res = httptest.NewRecorder()
	gMux.ServeHTTP(res, req)

	assert.Equal(t, 200, res.Code)

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo")}), resChan):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-resChan:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	req = httptest.NewRequest(http.MethodGet, "/outputs/foo", http.NoBody)
	res = httptest.NewRecorder()
	gMux.ServeHTTP(res, req)

	assert.Equal(t, 200, res.Code)
	assert.Equal(t, `label: ""
drop: {}
`, res.Body.String())

	o.TriggerCloseNow()
	require.NoError(t, o.WaitForClose(ctx))
}
