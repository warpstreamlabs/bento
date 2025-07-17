package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/config/test"
	"github.com/warpstreamlabs/bento/internal/docs"
	ifilepath "github.com/warpstreamlabs/bento/internal/filepath"
	"github.com/warpstreamlabs/bento/internal/manager"
)

// Keeps track of which resource file provided a given resource type, this is
// important when removing resources that have been deleted from a file, as it's
// possible it was moved to a new file and that update was reflected before this
// one.
type resourceSourceInfo struct {
	inputs     map[string]string
	processors map[string]string
	outputs    map[string]string
	caches     map[string]string
	rateLimits map[string]string
}

func newResourceSourceInfo() *resourceSourceInfo {
	return &resourceSourceInfo{
		inputs:     map[string]string{},
		processors: map[string]string{},
		outputs:    map[string]string{},
		caches:     map[string]string{},
		rateLimits: map[string]string{},
	}
}

func (r *resourceSourceInfo) populateFrom(path string, info *resourceFileInfo) {
	for k := range info.caches {
		r.caches[k] = path
	}
	for k := range info.inputs {
		r.inputs[k] = path
	}
	for k := range info.outputs {
		r.outputs[k] = path
	}
	for k := range info.processors {
		r.processors[k] = path
	}
	for k := range info.rateLimits {
		r.rateLimits[k] = path
	}
}

func (r *resourceSourceInfo) removeOwnedCache(ctx context.Context, label, path string, mgr bundle.NewManagement) {
	if r.caches[label] == path {
		if err := mgr.RemoveCache(ctx, label); err != nil {
			mgr.Logger().Error("Failed to remove deleted resource %v: %v", label, err)
		} else {
			delete(r.caches, label)
		}
	}
}

func (r *resourceSourceInfo) removeOwnedInput(ctx context.Context, label, path string, mgr bundle.NewManagement) {
	if r.inputs[label] == path {
		if err := mgr.RemoveInput(ctx, label); err != nil {
			mgr.Logger().Error("Failed to remove deleted resource %v: %v", label, err)
		} else {
			delete(r.inputs, label)
		}
	}
}

func (r *resourceSourceInfo) removeOwnedOutput(ctx context.Context, label, path string, mgr bundle.NewManagement) {
	if r.outputs[label] == path {
		if err := mgr.RemoveOutput(ctx, label); err != nil {
			mgr.Logger().Error("Failed to remove deleted resource %v: %v", label, err)
		} else {
			delete(r.outputs, label)
		}
	}
}

func (r *resourceSourceInfo) removeOwnedProcessor(ctx context.Context, label, path string, mgr bundle.NewManagement) {
	if r.processors[label] == path {
		if err := mgr.RemoveProcessor(ctx, label); err != nil {
			mgr.Logger().Error("Failed to remove deleted resource %v: %v", label, err)
		} else {
			delete(r.processors, label)
		}
	}
}

func (r *resourceSourceInfo) removeOwnedRateLimit(ctx context.Context, label, path string, mgr bundle.NewManagement) {
	if r.rateLimits[label] == path {
		if err := mgr.RemoveRateLimit(ctx, label); err != nil {
			mgr.Logger().Error("Failed to remove deleted resource %v: %v", label, err)
		} else {
			delete(r.rateLimits, label)
		}
	}
}

// Keeps track of which resources came from a file in its last read, if configs
// are changed, added or missing we need to reflect that.
type resourceFileInfo struct {
	inputs     map[string]*input.Config
	processors map[string]*processor.Config
	outputs    map[string]*output.Config
	caches     map[string]*cache.Config
	rateLimits map[string]*ratelimit.Config
}

func resInfoEmpty() resourceFileInfo {
	return resourceFileInfo{
		inputs:     map[string]*input.Config{},
		processors: map[string]*processor.Config{},
		outputs:    map[string]*output.Config{},
		caches:     map[string]*cache.Config{},
		rateLimits: map[string]*ratelimit.Config{},
	}
}

func resInfoFromConfig(conf *manager.ResourceConfig) resourceFileInfo {
	resInfo := resInfoEmpty()

	// New style
	for _, c := range conf.ResourceInputs {
		c := c
		resInfo.inputs[c.Label] = &c
	}
	for _, c := range conf.ResourceProcessors {
		c := c
		resInfo.processors[c.Label] = &c
	}
	for _, c := range conf.ResourceOutputs {
		c := c
		resInfo.outputs[c.Label] = &c
	}
	for _, c := range conf.ResourceCaches {
		c := c
		resInfo.caches[c.Label] = &c
	}
	for _, c := range conf.ResourceRateLimits {
		c := c
		resInfo.rateLimits[c.Label] = &c
	}

	return resInfo
}

func (r *Reader) resourcePathsExpanded() ([]string, error) {
	resourcePaths, err := ifilepath.Globs(r.fs, r.resourcePaths)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve resource glob pattern: %w", err)
	}
	for i, v := range resourcePaths {
		resourcePaths[i] = filepath.Clean(v)
	}
	return resourcePaths, nil
}

func (r *Reader) readResources(conf *manager.ResourceConfig) (lints []string, lintWarns []string, err error) {
	resourcesPaths, err := r.resourcePathsExpanded()
	if err != nil {
		return nil, nil, err
	}
	for _, path := range resourcesPaths {
		var rconf manager.ResourceConfig
		var rLints []string
		var rLintWarns []string
		if rconf, rLints, rLintWarns, err = r.readResource(path); err != nil {
			return
		}
		lints = append(lints, rLints...)
		lintWarns = append(lintWarns, rLintWarns...)

		resInfo := resInfoFromConfig(&rconf)
		r.resourceFileInfo[path] = resInfo
		r.resourceSources.populateFrom(path, &resInfo)

		if err = conf.AddFrom(&rconf); err != nil {
			err = fmt.Errorf("%v: %w", path, err)
			return
		}
	}
	return
}

func (r *Reader) readResource(path string) (conf manager.ResourceConfig, lints []string, lintWarns []string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%v: %w", path, err)
		}
	}()

	var confBytes []byte
	var dLints []docs.Lint
	var modTime time.Time
	if confBytes, dLints, modTime, err = ReadFileEnvSwap(r.fs, path, os.LookupEnv); err != nil {
		return
	}
	for _, l := range dLints {
		lints = append(lints, l.Error())
	}
	r.modTimeLastRead[path] = modTime

	var rawNode *yaml.Node
	if rawNode, err = docs.UnmarshalYAML(confBytes); err != nil {
		return
	}

	spec := append(docs.FieldSpecs{
		test.ConfigSpec(),
	}, r.specResources...)
	if !bytes.HasPrefix(confBytes, []byte("# BENTO LINT DISABLE")) {
		for _, lint := range spec.LintYAML(r.lintCtx(), rawNode) {
			switch lint.Level {
			case docs.LintWarning:
				lintWarns = append(lintWarns, fmt.Sprintf("%v%v", path, lint.Error()))
			default:
				lints = append(lints, fmt.Sprintf("%v%v", path, lint.Error()))
			}
		}
	}

	var pConf *docs.ParsedConfig
	if pConf, err = spec.ParsedConfigFromAny(rawNode); err != nil {
		return
	}

	conf, err = manager.FromParsed(r.lintConf.DocsProvider, pConf)
	return
}

// TriggerResourceUpdate attempts to re-read a resource configuration file and
// apply changes to the provided manager as appropriate.
func (r *Reader) TriggerResourceUpdate(mgr bundle.NewManagement, strict bool, path string) error {
	newResConf, lints, lintWarns, err := r.readResource(path)
	if errors.Is(err, fs.ErrNotExist) {
		return r.TriggerResourceDelete(mgr, path)
	}
	if err != nil {
		mgr.Logger().Error("Failed to read updated resources config: %v", err)
		return noReread(err)
	}

	prevInfo, exists := r.resourceFileInfo[path]
	if exists {
		mgr.Logger().Info("Resource %v config updated, attempting to update resources.", path)
	} else {
		prevInfo = resInfoEmpty()
		mgr.Logger().Info("Resource %v config created, attempting to add resources.", path)
	}

	lintlog := mgr.Logger()
	for _, lint := range lints {
		lintlog.Info(lint)
	}
	if strict && len(lints) > 0 {
		mgr.Logger().Error("Rejecting updated resource config due to linter errors, to allow linting errors run Bento with --chilled")
		return noReread(errors.New("file contained linting errors and is running in strict mode"))
	}

	for _, lintWarn := range lintWarns {
		lintlog.Warn(lintWarn)
	}

	newInfo := resInfoFromConfig(&newResConf)
	if err := r.applyResourceChanges(path, mgr, newInfo, prevInfo); err != nil {
		return err
	}

	r.resourceFileInfo[path] = newInfo
	return nil
}

// TriggerResourceDelete attempts to remove all resources that originated from a
// given file.
func (r *Reader) TriggerResourceDelete(mgr bundle.NewManagement, path string) error {
	prevInfo, exists := r.resourceFileInfo[path]
	if !exists {
		return nil
	}
	mgr.Logger().Info("Resource file %v deleted, attempting to remove resources.", path)

	newInfo := resInfoEmpty()
	if err := r.applyResourceChanges(path, mgr, newInfo, prevInfo); err != nil {
		return err
	}
	delete(r.resourceFileInfo, path)
	return nil
}

func (r *Reader) applyResourceChanges(path string, mgr bundle.NewManagement, currentInfo, prevInfo resourceFileInfo) error {
	// Kind of arbitrary, but I feel better about having some sort of timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// WARNING: The order here is actually kind of important, we want to start
	// with components that could be dependencies of other components. This is
	// a "best attempt", so not all edge cases need to be accounted for.

	unaccounted := map[string]struct{}{}
	for k := range prevInfo.rateLimits {
		unaccounted[k] = struct{}{}
	}
	for k, v := range currentInfo.rateLimits {
		delete(unaccounted, k)
		if err := mgr.StoreRateLimit(ctx, k, *v); err != nil {
			mgr.Logger().Error("Failed to update resource %v: %v", k, err)
			return fmt.Errorf("resource %v: %w", k, err)
		}
		mgr.Logger().Info("Updated resource %v config from file.", k)
	}
	for k := range unaccounted {
		r.resourceSources.removeOwnedRateLimit(ctx, k, path, mgr)
	}

	unaccounted = map[string]struct{}{}
	for k := range prevInfo.caches {
		unaccounted[k] = struct{}{}
	}
	for k, v := range currentInfo.caches {
		delete(unaccounted, k)
		if err := mgr.StoreCache(ctx, k, *v); err != nil {
			mgr.Logger().Error("Failed to update resource %v: %v", k, err)
			return fmt.Errorf("resource %v: %w", k, err)
		}
		mgr.Logger().Info("Updated resource %v config from file.", k)
	}
	for k := range unaccounted {
		r.resourceSources.removeOwnedCache(ctx, k, path, mgr)
	}

	unaccounted = map[string]struct{}{}
	for k := range prevInfo.processors {
		unaccounted[k] = struct{}{}
	}
	for k, v := range currentInfo.processors {
		delete(unaccounted, k)
		if err := mgr.StoreProcessor(ctx, k, *v); err != nil {
			mgr.Logger().Error("Failed to update resource %v: %v", k, err)
			return fmt.Errorf("resource %v: %w", k, err)
		}
		mgr.Logger().Info("Updated resource %v config from file.", k)
	}
	for k := range unaccounted {
		r.resourceSources.removeOwnedProcessor(ctx, k, path, mgr)
	}

	unaccounted = map[string]struct{}{}
	for k := range prevInfo.inputs {
		unaccounted[k] = struct{}{}
	}
	for k, v := range currentInfo.inputs {
		delete(unaccounted, k)
		if err := mgr.StoreInput(ctx, k, *v); err != nil {
			mgr.Logger().Error("Failed to update resource %v: %v", k, err)
			return fmt.Errorf("resource %v: %w", k, err)
		}
		mgr.Logger().Info("Updated resource %v config from file.", k)
	}
	for k := range unaccounted {
		r.resourceSources.removeOwnedInput(ctx, k, path, mgr)
	}

	unaccounted = map[string]struct{}{}
	for k := range prevInfo.outputs {
		unaccounted[k] = struct{}{}
	}
	for k, v := range currentInfo.outputs {
		delete(unaccounted, k)
		if err := mgr.StoreOutput(ctx, k, *v); err != nil {
			mgr.Logger().Error("Failed to update resource %v: %v", k, err)
			return fmt.Errorf("resource %v: %w", k, err)
		}
		mgr.Logger().Info("Updated resource %v config from file.", k)
	}
	for k := range unaccounted {
		r.resourceSources.removeOwnedOutput(ctx, k, path, mgr)
	}

	r.resourceSources.populateFrom(path, &currentInfo)
	return nil
}
