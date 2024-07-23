package pure

import (
	"context"
	"fmt"
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
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	wflowProcFieldMetaPathV2       = "metapath"
	wflowProcFieldDependencyListV2 = "dependency_list"
	wflowProcFieldBranchesV2       = "branches"
)

func workflowProcSpecV2() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Composition").
		Summary(`Executes a topology of `+"[`branch` processors][processors.branch]"+`, performing them in parallel where possible.`).
		Description(`
## workflow vs workflow_v2

The workflow_v2 processor is an evolution of the original `+"[`workflow` processor][processors.workflow]"+`. The two key differences are: a change to the way the topology of branch processors are defined & an enhancement that increases the parallelism of the DAG execution. Also, the original workflow processor has some features such as: implicitly creating the DAG based upon the request_map & result_map field of the branch processors, which have been dropped in workflow_v2. 

### Processor Topology Definition 

With the workflow_v2 processor you provide an explicit list of the dependencies for each node in the graph. Take the following for a simple example:

`+"```text"+`
     /--> B --\
A --|          |--> D
     \--> C --/
`+"```"+`

`+"```yaml"+`
pipeline:
  processors:
    - workflow_v2:
        branches:
          A:
            processors:
              - noop: {}

          B:
            dependency_list: ["A"]
            processors:
              - noop: {}

          C:
            dependency_list: ["A"]
            processors:
              - noop: {}

          D:
            dependency_list: ["B", "C"]
            processors:
              - noop: {}
`+"```"+`

### Execution Ordering

The workflow processor executes a DAG of branch processors, "performing them in parallel where possible". However the workflow processor uses a dependency solver that takes the approach: resolve the DAG into series of steps where the steps are performed sequentially but the processors in each step are performed in parallel. This means that there can be a situation where a step could be waiting for all the nodes in the previous step: _even though all dependencies for the step are ready_.

Consider the following DAG, from the workflow processor docs:

`+"```text"+`
      /--> B -------------|--> D
     /                   /
A --|          /--> E --|
     \--> C --|          \
               \----------|--> F
`+"```"+`

The dependency solver would resolve the DAG into a series of stages: 

`+"```text"+`
[ [ A ], [ B, C ], [ E ], [ D, F ] ]
`+"```"+`

Consider the Node E on the graph, we can see the that full dependency of this node would be : A -> C -> E, however in the stage before [ E ], there is the node B so in the original workflow processor, E would not execute until B has finished _even though there is no dependency of B for E_.

This workflow_v2 processor takes a different approach, a state for each node is maintained giving greater control to the execution of each node such that when a node's dependency list is fulfilled it will start executing. Also in the case where you are processing a batch of messages in the original workflow processor, each node in the DAG must process all messages in the batch before moving to the next stage, the workflow_v2 processor does not have this limitation.

`).
		Footnotes(`
[dag_wiki]: https://en.wikipedia.org/wiki/Directed_acyclic_graph
[processors.switch]: /docs/components/processors/switch
[processors.http]: /docs/components/processors/http
[processors.aws_lambda]: /docs/components/processors/aws_lambda
[processors.cache]: /docs/components/processors/cache
[processors.branch]: /docs/components/processors/branch
[processors.workflow]: /docs/components/processors/workflow
[guides.bloblang]: /docs/guides/bloblang/about
[configuration.pipelines]: /docs/configuration/processing_pipelines
[configuration.error-handling]: /docs/configuration/error_handling
[configuration.resources]: /docs/configuration/resources
`).
		Fields(
			service.NewStringField(wflowProcFieldMetaPathV2).
				Description("A [dot path](/docs/configuration/field_paths) indicating where to store and reference structured metadata about the workflow_v2 execution.").
				Default("meta.workflow_v2"),
			service.NewObjectMapField(wflowProcFieldBranchesV2, workflowv2BranchSpecFields()...).
				Description("An object of named [`branch` processors](/docs/components/processors/branch) that make up the workflow_v2."))
}

func workflowv2BranchSpecFields() []*service.ConfigField {
	branchSpecFields := branchSpecFields()
	dependencyList := service.NewStringListField(wflowProcFieldDependencyListV2).
		Description("This is a list of nodes that this node is dependent upon.").
		Default([]string{})
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

// WorkflowV2 is a processor that applies a list of child processors to a new
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

// NewWorkflowV2 instanciates a new workflow_v2 processor.
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
	skipped           []map[string]struct{}
	failed            []map[string]string
	dependencyTracker []map[string][]string
	numberOfBranches  int
	sync.Mutex
}

func createTrackerFromDependencies(dependencies map[string][]string, skipList map[string]struct{}, msg message.Batch) *resultTrackerV2 {
	r := &resultTrackerV2{
		notStarted:        make([]map[string]struct{}, msg.Len()),
		started:           make([]map[string]struct{}, msg.Len()),
		succeeded:         make([]map[string]struct{}, msg.Len()),
		skipped:           make([]map[string]struct{}, msg.Len()),
		failed:            make([]map[string]string, msg.Len()),
		dependencyTracker: make([]map[string][]string, msg.Len()),
	}

	for i := 0; i < msg.Len(); i++ {
		r.notStarted[i] = make(map[string]struct{})
		r.started[i] = make(map[string]struct{})
		r.succeeded[i] = make(map[string]struct{})
		r.skipped[i] = make(map[string]struct{})
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

	for i := 0; i < msg.Len(); i++ {
		for k := range skipList {
			r.Skipped(i, k)
			r.RemoveFromDepTracker(i, k)
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

func (r *resultTrackerV2) Skipped(i int, k string) {
	r.Lock()
	delete(r.notStarted[i], k)

	r.skipped[i][k] = struct{}{}
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

func (r *resultTrackerV2) isFinished(batchSize int) bool {
	r.Lock()
	defer r.Unlock()
	for i := 0; i < batchSize; i++ {
		if len(r.succeeded[i])+len(r.failed[i])+len(r.skipped[i]) != r.numberOfBranches {
			return true
		}
	}
	return false
}

func (r *resultTrackerV2) getAReadyBranch(batchSize int) (int, string, bool) {
	r.Lock()
	defer r.Unlock()
	for i := 0; i < batchSize; i++ {
		for eid := range r.notStarted[i] {
			if len(r.dependencyTracker[i][eid]) == 0 {
				return i, eid, false
			}
		}
	}
	return 0, "", true
}

func (r *resultTrackerV2) ToObjectV2(i int) map[string]any {
	succeeded := make([]any, 0, len(r.succeeded[i]))
	notStarted := make([]any, 0, len(r.notStarted[i]))
	started := make([]any, 0, len(r.started[i]))
	skipped := make([]any, 0, len(r.skipped[i]))
	failed := make(map[string]any, len(r.failed))

	for k := range r.succeeded[i] {
		succeeded = append(succeeded, k)
	}
	sort.Slice(succeeded, func(i, j int) bool {
		return succeeded[i].(string) < succeeded[j].(string)
	})
	for k := range r.notStarted[i] {
		notStarted = append(notStarted, k)
	}
	for k := range r.started[i] {
		started = append(started, k)
	}
	for k := range r.skipped[i] {
		skipped = append(skipped, k)
	}
	for k, v := range r.failed[i] {
		failed[k] = v
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
	if len(skipped) > 0 {
		m["skipped"] = skipped
	}
	if len(failed) > 0 {
		m["failed"] = failed
	}
	return m
}

// Returns a map of enrichment IDs that should be skipped for this payload.
func (w *WorkflowV2) skipFromMetaV2(root any) map[string]struct{} {
	skipList := map[string]struct{}{}
	if len(w.metaPath) == 0 {
		return skipList
	}

	gObj := gabs.Wrap(root)

	// If a whitelist is provided for this flow then skip stages that aren't
	// within it.
	if apply, ok := gObj.S(append(w.metaPath, "apply")...).Data().([]any); ok {
		if len(apply) > 0 {
			for k := range w.allStages {
				skipList[k] = struct{}{}
			}
			for _, id := range apply {
				if idStr, isString := id.(string); isString {
					delete(skipList, idStr)
				}
			}
		}
	}

	// Skip stages that already succeeded in a previous run of this workflow.
	if succeeded, ok := gObj.S(append(w.metaPath, "succeeded")...).Data().([]any); ok {
		for _, id := range succeeded {
			if idStr, isString := id.(string); isString {
				if _, exists := w.allStages[idStr]; exists {
					skipList[idStr] = struct{}{}
				}
			}
		}
	}

	// Skip stages that were already skipped in a previous run of this workflow.
	if skipped, ok := gObj.S(append(w.metaPath, "skipped")...).Data().([]any); ok {
		for _, id := range skipped {
			if idStr, isString := id.(string); isString {
				if _, exists := w.allStages[idStr]; exists {
					skipList[idStr] = struct{}{}
				}
			}
		}
	}

	return skipList
}

// ProcessBatch applies workflow_v2 stages to each part of a message type.
func (w *WorkflowV2) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	w.mReceived.Incr(int64(msg.Len()))
	w.mBatchReceived.Incr(1)
	startedAt := time.Now()

	// Prevent resourced branches from being updated mid-flow.
	children, dependencies, unlock, err := w.children.LockV2()
	if err != nil {
		w.mError.Incr(1)
		w.log.Error("Failed to establish workflow_v2: %v\n", err)

		_ = msg.Iter(func(i int, p *message.Part) error {
			p.ErrorSet(err)
			return nil
		})
		w.mSent.Incr(int64(msg.Len()))
		w.mBatchSent.Incr(1)
		return []message.Batch{msg}, nil
	}
	defer unlock()

	skipOnMeta := make(map[string]struct{}, 1)
	_ = msg.Iter(func(i int, p *message.Part) error {
		if jObj, err := p.AsStructured(); err == nil {
			skipOnMeta = w.skipFromMetaV2(jObj)
		} else {
			skipOnMeta = map[string]struct{}{}
		}
		return nil
	})

	records := createTrackerFromDependencies(dependencies, skipOnMeta, msg)

	type collector struct {
		eid        string
		mssgPartID int
		results    [][]*message.Part
		errors     []error
	}

	batchResultChan := make(chan collector)

	go func() {
		for {
			mssge := <-batchResultChan
			var failed []branchMapError
			err := mssge.errors[mssge.mssgPartID]

			// bodgy
			resultsBodge := make([]*message.Part, msg.Len())
			xxx := mssge.results[0]
			resultsBodge[mssge.mssgPartID] = xxx[0]

			if err == nil {
				failed, err = children[mssge.eid].overlayResult(msg, resultsBodge)
			}
			if err != nil {
				w.mError.Incr(1)
				w.log.Error("Failed to perform enrichment '%v': %v\n", mssge.eid, err)
				records.FailedV2(mssge.mssgPartID, mssge.eid, err.Error())
			}
			for _, e := range failed {
				records.FailedV2(mssge.mssgPartID, mssge.eid, e.err.Error())
			}
		}
	}()

	for records.isFinished(msg.Len()) {
		i, eid, isNonFound := records.getAReadyBranch(msg.Len())
		if isNonFound {
			continue
		}
		records.Started(i, eid)

		results := make([][]*message.Part, msg.Len())
		errors := make([]error, msg.Len())

		branchMsg := msg.ShallowCopy()

		go func(i int, id string) {

			branchParts := make([]*message.Part, branchMsg.Len())
			_ = branchMsg.Iter(func(partIndex int, part *message.Part) error {
				// Remove errors so that they aren't propagated into the
				// branch.
				part.ErrorSet(nil)
				branchParts[partIndex] = part
				return nil
			})

			var mapErrs []branchMapError

			// bodgy
			testPart := branchParts[i]
			xxxPart := make([]*message.Part, 1)
			xxxPart[0] = testPart

			results[0], mapErrs, errors[0] = children[id].createResult(ctx, xxxPart, msg.ShallowCopy())

			records.Succeeded(i, id)
			for _, e := range mapErrs {
				records.FailedV2(i, id, e.err.Error())
			}
			batchResult := collector{
				eid:        id,
				mssgPartID: i,
				results:    results,
				errors:     errors,
			}
			batchResultChan <- batchResult
		}(i, eid)
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
			current := records.ToObjectV2(i)
			if previous != nil {
				current["previous"] = previous
			}
			_, _ = gObj.Set(current, w.metaPath...)

			p.SetStructuredMut(gObj.Data())
			return nil
		})
	} else {
		_ = msg.Iter(func(i int, p *message.Part) error {
			for _, m := range records.failed {
				for _, value := range m {
					p.ErrorSet(fmt.Errorf("workflow_v2 branches failed: %v", value))
				}
			}
			return nil
		})
	}

	w.mSent.Incr(int64(msg.Len()))
	w.mBatchSent.Incr(1)
	w.mLatency.Timing(time.Since(startedAt).Nanoseconds())

	return []message.Batch{msg}, nil
}

// Close shuts down the processor and stops processing requests.
func (w *WorkflowV2) Close(ctx context.Context) error {
	return w.children.Close(ctx)
}
