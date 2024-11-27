//go:build GO

package huggingface

import (
	"os"
	"sync"

	"github.com/knights-analytics/hugot"
	"github.com/warpstreamlabs/bento/public/service"
	ort "github.com/yalue/onnxruntime_go"
)

type ortSession struct {
	mut     sync.Mutex
	session *hugot.Session
}

func (o *ortSession) Get() *hugot.Session {
	o.mut.Lock()
	session := o.session
	o.mut.Unlock()
	return session
}

func (o *ortSession) Destroy() {
	o.mut.Lock()
	o.session.Destroy()
	o.mut.Unlock()
}

func (o *ortSession) NewSession() (*hugot.Session, error) {
	o.mut.Lock()
	defer o.mut.Unlock()

	if o.session == nil || !ort.IsInitialized() {
		session, err := hugot.NewGoSession()
		if err != nil {
			return nil, err
		}
		o.session = session
	}

	return o.session, nil
}

func (o *ortSession) DownloadModel(logger *service.Logger, modelName string, destination string, options hugot.DownloadOptions) (string, error) {
	// Hacky workaround since the DownloadModel prints to stdout. Currently disabled by default.
	tempStdOut := os.Stdout
	os.Stdout = nil
	defer func() {
		os.Stdout = tempStdOut
	}()
	path, err := o.session.DownloadModel(modelName, destination, options)
	return path, err
}

var globalSession = &ortSession{
	session: nil,
	mut:     sync.Mutex{},
}
