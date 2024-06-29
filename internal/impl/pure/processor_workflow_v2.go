package pure

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
	"go.opentelemetry.io/otel/trace"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/tracing"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	wflowProcFieldMetaPathV2       = "metapath"
	wflowProcFieldDependencyListV2 = "dependency_list"
	wflowProcFieldBranchesV2       = "branches"
)

func workflowProcSpecV2() *service.ConfigSpec {
	return service.NewConfigSpec().
		Fields(
			service.NewStringField(wflowProcFieldMetaPathV2).
				Description("A [dot path](/docs/configuration/field_paths) indicating where to store and reference [structured metadata](#structured-metadata) about the workflow execution.").
				Default("meta.workflow"),
			service.NewObjectMapField(wflowProcFieldBranchesV2, workflowBranchSpecFields()...).
				Description("An object of named [`branch` processors](/docs/components/processors/branch) that make up the workflow. The order and parallelism in which branches are executed can either be made explicit with the field `order`, or if omitted an attempt is made to automatically resolve an ordering based on the mappings of each branch."))
}

func workflowBranchSpecFields() []*service.ConfigField {
	branchSpecFields := branchSpecFields()
	dependencyList := service.NewStringListField(wflowProcFieldDependencyListV2)
	workflowV2BranchSpecFields := append(branchSpecFields, dependencyList)
	return workflowV2BranchSpecFields
}

func init() {
	err := service.RegisterBatchProcessor(
		"workflow_v2", workflowProcSpecV2(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			w, err := NewWorkflowV2(conf, interop.UnwrapManagement(mgr))
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(w), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// Workflow is a processor that applies a list of child processors to a new TODO
// payload mapped from the original, and after processing attempts to overlay
// the results back onto the original payloads according to more mappings.
type WorkflowV2 struct {
	log    log.Modular
	tracer trace.TracerProvider

	children  *workflowBranchMapV2
	allStages map[string]struct{}
	metaPath  []string

	// Metrics
	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

// NewWorkflow instanciates a new workflow processor. TODO
func NewWorkflowV2(conf *service.ParsedConfig, mgr bundle.NewManagement) (*WorkflowV2, error) {

	stats := mgr.Metrics()
	w := &WorkflowV2{
		log:    mgr.Logger(),
		tracer: mgr.Tracer(),

		metaPath:  nil,
		allStages: map[string]struct{}{},

		mReceived:      stats.GetCounter("processor_received"),
		mBatchReceived: stats.GetCounter("processor_batch_received"),
		mSent:          stats.GetCounter("processor_sent"),
		mBatchSent:     stats.GetCounter("processor_batch_sent"),
		mError:         stats.GetCounter("processor_error"),
		mLatency:       stats.GetTimer("processor_latency_ns"),
	}

	metaStr, err := conf.FieldString(wflowProcFieldMetaPathV2)
	if err != nil {
		return nil, err
	}
	if metaStr != "" {
		w.metaPath = gabs.DotPathToSlice(metaStr)
	}

	if w.children, err = newWorkflowBranchMapV2(conf, mgr); err != nil {
		return nil, err
	}
	for k := range w.children.Branches {
		w.allStages[k] = struct{}{}
	}

	return w, nil

}

type resultTrackerV2 struct {
	notStarted        []map[string]struct{}
	started           []map[string]struct{}
	succeeded         []map[string]struct{}
	failed            []map[string]string
	dependencyTracker []map[string][]string
	numberOfBranches  int
	sync.Mutex
}

func createTrackerFromDependencies(dependencies map[string][]string, batch_size int) *resultTrackerV2 {
	r := &resultTrackerV2{
		notStarted:        make([]map[string]struct{}, batch_size),
		started:           make([]map[string]struct{}, batch_size),
		succeeded:         make([]map[string]struct{}, batch_size),
		failed:            make([]map[string]string, batch_size),
		dependencyTracker: make([]map[string][]string, batch_size),
	}

	for i := 0; i < batch_size; i++ {
		r.notStarted[i] = make(map[string]struct{})
		r.started[i] = make(map[string]struct{})
		r.succeeded[i] = make(map[string]struct{})
		r.failed[i] = make(map[string]string)

		for k := range dependencies {
			r.notStarted[i][k] = struct{}{}
		}
	}

	r.numberOfBranches = len(dependencies)

	for x := range r.dependencyTracker {
		r.dependencyTracker[x] = make(map[string][]string)
		for k, v := range dependencies {
			r.dependencyTracker[x][k] = v
		}
	}

	return r
}

func (r *resultTrackerV2) Succeeded(i int, k string) {
	r.Lock()
	delete(r.started[i], k)

	r.succeeded[i][k] = struct{}{}
	r.RemoveFromDepTracker(i, k)
	r.Unlock()
}

func (r *resultTrackerV2) Started(i int, k string) {
	r.Lock()
	delete(r.notStarted[i], k)

	r.started[i][k] = struct{}{}
	r.Unlock()
}

func (r *resultTrackerV2) FailedV2(i int, k string, why string) {
	r.Lock()
	delete(r.started[i], k)
	delete(r.succeeded[i], k)
	r.failed[i][k] = why
	r.Unlock()
}

func (r *resultTrackerV2) RemoveFromDepTracker(i int, k string) {
	for key, values := range r.dependencyTracker[i] {
		var updatedValues []string
		for _, value := range values {
			if value != k {
				updatedValues = append(updatedValues, value)
			}
		}
		r.dependencyTracker[i][key] = updatedValues
	}
}

func (r *resultTrackerV2) isReadyToStart(i int, k string) bool {
	r.Lock()
	defer r.Unlock()
	return len(r.dependencyTracker[i][k]) == 0
}
func (r *resultTrackerV2) ToObjectV2(i int) map[string]any {
	succeeded := make([]any, 0, len(r.succeeded[0]))
	notStarted := make([]any, 0, len(r.notStarted[0]))
	started := make([]any, 0, len(r.started[0]))
	failed := make([]map[string]any, len(r.failed[0]))

	for k := range r.succeeded[0] {
		succeeded = append(succeeded, k)
	}
	sort.Slice(succeeded, func(i, j int) bool {
		return succeeded[i].(string) < succeeded[j].(string)
	})
	for k := range r.notStarted[0] {
		notStarted = append(notStarted, k)
	}
	for k := range r.started[0] {
		started = append(started, k)
	}

	if len(r.failed) > 0 {
		failed[0] = make(map[string]any)
		for k, v := range r.failed[0] {
			failed[0][k] = v
		}
	}

	m := map[string]any{}
	if len(succeeded) > 0 {
		m["succeeded"] = succeeded
	}
	if len(notStarted) > 0 {
		m["notStarted"] = notStarted
	}
	if len(started) > 0 {
		m["started"] = started
	}
	if len(failed) > 0 && len(failed[0]) > 0 {
		m["failed"] = failed
	}
	return m
}

// ProcessBatch applies workflow stages to each part of a message type.
func (w *WorkflowV2) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	w.mReceived.Incr(int64(msg.Len()))
	w.mBatchReceived.Incr(1)
	startedAt := time.Now()

	// Prevent resourced branches from being updated mid-flow.
	children, dependencies, unlock, err := w.children.LockV2()
	if err != nil {
		w.mError.Incr(1)
		w.log.Error("Failed to establish workflow: %v\n", err)

		_ = msg.Iter(func(i int, p *message.Part) error {
			p.ErrorSet(err)
			return nil
		})
		w.mSent.Incr(int64(msg.Len()))
		w.mBatchSent.Incr(1)
		return []message.Batch{msg}, nil
	}
	defer unlock()

	skipOnMeta := make([]map[string]struct{}, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		skipOnMeta[i] = map[string]struct{}{}
		return nil
	})

	propMsg, _ := tracing.WithChildSpans(w.tracer, "workflow", msg)

	records := createTrackerFromDependencies(dependencies, msg.Len())

	type collector struct {
		eid     string
		results [][]*message.Part
		errors  []error
	}

	batchResultChan := make(chan collector)

	go func() {
		mssge := <-batchResultChan
		var failed []branchMapError
		err := mssge.errors[0]
		if err == nil {
			failed, err = children[mssge.eid].overlayResult(msg, mssge.results[0])
		}
		if err != nil {
			w.mError.Incr(1)
			w.log.Error("Failed to perform enrichment '%v': %v\n", mssge.eid, err)
			records.FailedV2(0, mssge.eid, err.Error())
		}
		for _, e := range failed {
			records.FailedV2(0, mssge.eid, e.err.Error())
		}
	}()

	for len(records.succeeded[0])+len(records.failed[0]) != records.numberOfBranches { // while there is stuff to do...
		for eid := range records.notStarted[0] {

			results := make([][]*message.Part, 1)
			errors := make([]error, 1)

			if records.isReadyToStart(0, eid) { // get one that is ready to start...
				records.Started(0, eid) // record as started...

				branchMsg, branchSpans := tracing.WithChildSpans(w.tracer, eid, propMsg.ShallowCopy())

				go func(id string) {
					branchParts := make([]*message.Part, branchMsg.Len())
					_ = branchMsg.Iter(func(partIndex int, part *message.Part) error {
						// Remove errors so that they aren't propagated into the
						// branch.
						part.ErrorSet(nil)
						branchParts[partIndex] = part
						return nil
					})

					var mapErrs []branchMapError
					results[0], mapErrs, errors[0] = children[id].createResult(ctx, branchParts, propMsg.ShallowCopy())
					for _, s := range branchSpans {
						s.Finish()
					}
					records.Succeeded(0, id)
					for _, e := range mapErrs {
						records.FailedV2(0, id, e.err.Error())
					}
					batchResult := collector{
						eid:     id,
						results: results,
						errors:  errors,
					}
					batchResultChan <- batchResult
				}(eid)
			}
		}
	}

	// Finally, set the meta records of each document.
	if len(w.metaPath) > 0 {
		_ = msg.Iter(func(i int, p *message.Part) error {
			pJSON, err := p.AsStructuredMut()
			if err != nil {
				w.mError.Incr(1)
				w.log.Error("Failed to parse message for meta update: %v\n", err)
				p.ErrorSet(err)
				return nil
			}

			gObj := gabs.Wrap(pJSON)
			previous := gObj.S(w.metaPath...).Data()
			current := records.ToObjectV2(0)
			if previous != nil {
				current["previous"] = previous
			}
			_, _ = gObj.Set(current, w.metaPath...)

			p.SetStructuredMut(gObj.Data())
			return nil
		})
	} else {
		// _ = msg.Iter(func(i int, p *message.Part) error {
		// 	if lf := len(records.failed); lf > 0 {
		// 		failed := make([]string, 0, lf)
		// 		for k := range records.failed {
		// 			failed = append(failed, k)
		// 		}
		// 		sort.Strings(failed)
		// 		p.ErrorSet(fmt.Errorf("workflow branches failed: %v", failed))
		// 	}
		// 	return nil
		// })
	}

	tracing.FinishSpans(propMsg)

	w.mSent.Incr(int64(msg.Len()))
	w.mBatchSent.Incr(1)
	w.mLatency.Timing(time.Since(startedAt).Nanoseconds())

	return []message.Batch{msg}, nil
}

// Close shuts down the processor and stops processing requests.
func (w *WorkflowV2) Close(ctx context.Context) error {
	return w.children.Close(ctx)
}
