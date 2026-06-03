package protobuf

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	connectrpc "connectrpc.com/connect"
	"github.com/bufbuild/prototransform"
	"github.com/bufbuild/prototransform/leaser"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/warpstreamlabs/bento/public/service"
)

type MultiModuleWatcher struct {
	bsrClients map[string]*prototransform.SchemaWatcher
}

var _ prototransform.Resolver = &MultiModuleWatcher{}

func newMultiModuleWatcher(bsrModules []*service.ParsedConfig, mgr *service.Resources) (*MultiModuleWatcher, error) {
	if len(bsrModules) == 0 {
		return nil, errors.New("no modules provided")
	}
	multiModuleWatcher := &MultiModuleWatcher{}

	// Initialise one client for each module
	multiModuleWatcher.bsrClients = make(map[string]*prototransform.SchemaWatcher)
	for _, bsrModule := range bsrModules {
		var bsrURL string
		bsrURL, err := bsrModule.FieldString(fieldBSRUrl)
		if err != nil {
			return nil, err
		}

		var bsrAPIKey string
		if bsrAPIKey, err = bsrModule.FieldString(fieldBsrAPIKey); err != nil {
			return nil, err
		}

		var module string
		if module, err = bsrModule.FieldString(fieldBsrModule); err != nil {
			return nil, err
		}

		var version string
		if version, err = bsrModule.FieldString(fieldBsrVersion); err != nil {
			return nil, err
		}

		var cacheName string
		if cacheName, err = bsrModule.FieldString(fieldBsrCache); err != nil {
			return nil, err
		}

		var pollingPeriod time.Duration
		if pollingPeriod, err = bsrModule.FieldDuration(fieldBsrPollingPeriod); err != nil {
			return nil, err
		}

		var leaser *bsrPollingLeaser
		var cache *bsrCache
		if cacheName != "" {
			leaser = newLeaser(mgr, module, cacheName,
				func() {
					mgr.Logger().Infof("Acquired Lease for module: %v & cache: %v", module, cacheName)
				},
				func() {
					mgr.Logger().Infof("Released Lease for module: %v & cache: %v", module, cacheName)
				},
			)

			cache = newCache(mgr, cacheName)
		}

		watcher, err := newSchemaWatcher(context.Background(), bsrURL, bsrAPIKey, module, version, cache, leaser, pollingPeriod)
		if err != nil {
			return nil, err
		}
		multiModuleWatcher.bsrClients[module] = watcher
	}

	return multiModuleWatcher, nil
}

func newSchemaWatcher(ctx context.Context, bsrURL, bsrAPIKey, module, version string, cache *bsrCache, leaser *bsrPollingLeaser, pollingPeriod time.Duration) (*prototransform.SchemaWatcher, error) {
	// If no BSR url provided, extract from module
	if bsrURL == "" {
		segments := strings.Split(module, "/")
		if len(segments) != 3 {
			return nil, fmt.Errorf("could not parse module %s, expected three segments e.g. 'buf.build/exampleco/mymodule'", module)
		}
		bsrURL = "https://" + segments[0]
	}

	opts := []connectrpc.ClientOption{
		connectrpc.WithHTTPGet(),
		connectrpc.WithHTTPGetMaxURLSize(8192, true)}

	if bsrAPIKey != "" {
		opts = append(opts, connectrpc.WithInterceptors(prototransform.NewAuthInterceptor(bsrAPIKey)))
	}
	client := reflectv1beta1connect.NewFileDescriptorSetServiceClient(http.DefaultClient, bsrURL, opts...)

	cfg := &prototransform.SchemaWatcherConfig{
		SchemaPoller: prototransform.NewSchemaPoller(
			client,
			module,
			version,
		),
		Jitter:        0.2,
		PollingPeriod: pollingPeriod,
	}

	if cache != nil {
		cfg.Leaser = leaser
		cfg.Cache = cache
	}

	watcher, err := prototransform.NewSchemaWatcher(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema watcher: %w", err)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = watcher.AwaitReady(ctxWithTimeout); err != nil {
		return nil, fmt.Errorf("schema watcher never became ready: %w", err)
	}

	return watcher, nil
}

func (w *MultiModuleWatcher) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	for _, schemaWatcher := range w.bsrClients {
		extensionType, err := schemaWatcher.FindExtensionByName(field)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return extensionType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", field)
}

func (w *MultiModuleWatcher) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	for _, schemaWatcher := range w.bsrClients {
		extensionType, err := schemaWatcher.FindExtensionByNumber(message, field)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return extensionType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", message)
}

func (w *MultiModuleWatcher) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	for _, schemaWatcher := range w.bsrClients {
		messageType, err := schemaWatcher.FindMessageByName(message)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return messageType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", message)
}

func (w *MultiModuleWatcher) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	for _, schemaWatcher := range w.bsrClients {
		messageType, err := schemaWatcher.FindMessageByURL(url)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return messageType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", url)
}

func (w *MultiModuleWatcher) FindEnumByName(enum protoreflect.FullName) (protoreflect.EnumType, error) {
	for _, schemaWatcher := range w.bsrClients {
		enumType, err := schemaWatcher.FindEnumByName(enum)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return enumType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", enum)
}

//------------------------------------------------------------------------------

var (
	cacheMap   map[string]*bsrCache = make(map[string]*bsrCache)
	cacheMapMu sync.Mutex
)

// bsrCache implements prototransform.Cache
var _ prototransform.Cache = &bsrCache{}

type bsrCache struct {
	cacheName string
	mgr       *service.Resources
}

func newCache(mgr *service.Resources, cacheName string) *bsrCache {
	cacheMapMu.Lock()
	defer cacheMapMu.Unlock()
	if c, ok := cacheMap[cacheName]; ok {
		return c
	}

	c := &bsrCache{
		cacheName: cacheName,
		mgr:       mgr,
	}
	cacheMap[cacheName] = c
	return c
}

func (bc *bsrCache) Load(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	var cacheErr error
	err := bc.mgr.AccessCache(ctx, bc.cacheName, func(c service.Cache) {
		data, cacheErr = c.Get(ctx, key)
	})
	if err == nil {
		err = cacheErr
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (bc *bsrCache) Save(ctx context.Context, key string, data []byte) error {
	var cacheErr error
	err := bc.mgr.AccessCache(ctx, bc.cacheName, func(c service.Cache) {
		cacheErr = c.Set(ctx, key, data, nil)
	})
	if err == nil {
		err = cacheErr
	}
	return err
}

//------------------------------------------------------------------------------

// leaseStore implements LeaseStore from the prototransform module
var _ leaser.LeaseStore = &leaseStore{}

type leaseStore struct {
	mgr       *service.Resources
	cacheName string
}

func (ls *leaseStore) TryAcquire(ctx context.Context, leaseName string, holderId []byte, ttl time.Duration) (created bool, currentHolder []byte, err error) {
	var cacheErr error
	err = ls.mgr.AccessCache(ctx, ls.cacheName, func(c service.Cache) {
		cacheErr = c.Add(ctx, leaseName, holderId, &ttl)
		if cacheErr == nil {
			created = true
			currentHolder = holderId
			return
		}
		if errors.Is(cacheErr, service.ErrKeyAlreadyExists) {
			created = false
			cacheErr = nil
			return
		} else if cacheErr != nil {
			created = false
			return
		}
	})
	if err == nil {
		err = cacheErr
	}
	if err != nil {
		return false, nil, err
	}

	if !created {
		err = ls.mgr.AccessCache(ctx, ls.cacheName, func(c service.Cache) {
			currentHolder, cacheErr = c.Get(ctx, leaseName)
		})
		if err == nil {
			err = cacheErr
		}
		if err != nil {
			return false, nil, err
		}
	}

	return created, currentHolder, err
}

func (ls *leaseStore) Release(ctx context.Context, leaseName string, holderId []byte) error {
	var cacheErr error

	err := ls.mgr.AccessCache(ctx, ls.cacheName, func(c service.Cache) {
		var currentHolder []byte
		currentHolder, cacheErr = c.Get(ctx, leaseName)
		if errors.Is(cacheErr, service.ErrKeyNotFound) {
			cacheErr = nil
			return
		}
		if cacheErr != nil {
			return
		}

		if !bytes.Equal(currentHolder, holderId) {
			return
		}

		cacheErr = c.Delete(ctx, leaseName)
		if errors.Is(cacheErr, service.ErrKeyNotFound) {
			cacheErr = nil
		}
	})

	if err == nil {
		err = cacheErr
	}
	return err
}

//------------------------------------------------------------------------------

// bsrPollingLeaser implements prototransform.Leaser
var _ prototransform.Leaser = &bsrPollingLeaser{}

var (
	leaserMap   map[string]*bsrPollingLeaser = make(map[string]*bsrPollingLeaser)
	leaserMapMu sync.Mutex
)

type bsrPollingLeaser struct {
	leaseStore    leaser.LeaseStore
	leaseTTL      time.Duration
	pollingPeriod time.Duration
	onAcquire     func()
	onRelease     func()
}

func newLeaser(mgr *service.Resources, module, cacheName string, onAcquire, onRelease func()) *bsrPollingLeaser {
	leaserMapMu.Lock()
	defer leaserMapMu.Unlock()
	if l, ok := leaserMap[module+"::"+cacheName]; ok {
		return l
	}

	l := &bsrPollingLeaser{
		leaseStore: &leaseStore{
			mgr:       mgr,
			cacheName: cacheName,
		},
		leaseTTL:      time.Minute * 5,
		pollingPeriod: time.Minute,
		onAcquire:     onAcquire,
		onRelease:     onRelease,
	}
	leaserMap[module+"::"+cacheName] = l
	return l
}

func (bpl *bsrPollingLeaser) NewLease(ctx context.Context, leaseName string, leaseHolder []byte) prototransform.Lease {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	newLease := &lease{
		cancel:    cancel,
		done:      done,
		err:       prototransform.ErrLeaseStateNotYetKnown,
		onAcquire: bpl.onAcquire,
		onRelease: bpl.onRelease,
	}
	go newLease.run(ctx, bpl, leaseName, leaseHolder, done)
	return newLease
}

//------------------------------------------------------------------------------
// This lease implementation has been taken from:
// https://github.com/bufbuild/prototransform/blob/main/leaser/polling_leaser.go
//------------------------------------------------------------------------------

// lease implements prototransform.Lease
var _ prototransform.Lease = &lease{}

type lease struct {
	cancel context.CancelFunc
	done   <-chan struct{}

	mu     sync.Mutex
	isHeld bool
	err    error

	notifyMu             sync.Mutex
	onAcquire, onRelease func()
}

func (l *lease) IsHeld() (bool, error) {
	l.mu.Lock()
	isHeld, err := l.isHeld, l.err
	l.mu.Unlock()
	return isHeld, err
}

func (l *lease) SetCallbacks(onAcquire, onRelease func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onAcquire, l.onRelease = onAcquire, onRelease
	if l.isHeld && l.onAcquire != nil {
		go func() {
			l.notifyMu.Lock()
			defer l.notifyMu.Unlock()
			l.onAcquire()
		}()
	}
}

func (l *lease) Cancel() {
	l.cancel()
	<-l.done
}

func (l *lease) run(ctx context.Context, leaser *bsrPollingLeaser, key string, value []byte, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(leaser.pollingPeriod)
	defer ticker.Stop()
	l.poll(ctx, leaser, key, value)
	for {
		select {
		case <-ctx.Done():
			l.releaseNow(ctx, leaser, key, value)
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				// skip polling if context is done
				l.releaseNow(ctx, leaser, key, value)
				return
			}
			l.poll(ctx, leaser, key, value)
		}
	}
}

func (l *lease) poll(ctx context.Context, leaser *bsrPollingLeaser, key string, value []byte) {
	created, holder, err := leaser.leaseStore.TryAcquire(ctx, key, value, leaser.leaseTTL)
	if err != nil {
		l.released(err)
		return
	}
	if created {
		l.acquired()
		return
	}
	if bytes.Equal(holder, value) {
		// The existing lease is ours
		l.acquired()
		return
	}
	// The existing lease is not ours
	l.released(nil)
}

func (l *lease) releaseNow(ctx context.Context, leaser *bsrPollingLeaser, key string, value []byte) {
	l.mu.Lock()
	isHeld := l.isHeld
	l.mu.Unlock()
	if isHeld {
		// best effort: immediately release if we hold it
		_ = leaser.leaseStore.Release(ctx, key, value)
	}
	l.released(nil)
}

func (l *lease) acquired() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isHeld && l.onAcquire != nil {
		go func() {
			l.notifyMu.Lock()
			defer l.notifyMu.Unlock()
			l.onAcquire()
		}()
	}
	l.isHeld = true
	l.err = nil
}

func (l *lease) released(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isHeld && l.onRelease != nil {
		go func() {
			l.notifyMu.Lock()
			defer l.notifyMu.Unlock()
			l.onRelease()
		}()
	}
	l.isHeld = false
	l.err = err
}
