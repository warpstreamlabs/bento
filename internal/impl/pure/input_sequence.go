package pure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/OneOfOne/xxhash"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	siFieldShardedJoinType          = "type"
	siFieldShardedJoinIDPath        = "id_path"
	siFieldShardedJoinIterations    = "iterations"
	siFieldShardedJoinMergeStrategy = "merge_strategy"
	siFieldShardedJoin              = "sharded_join"
	siFieldInputs                   = "inputs"
)

func sequenceInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary("Reads messages from a sequence of child inputs, starting with the first and once that input gracefully terminates starts consuming from the next, and so on.").
		Description("This input is useful for consuming from inputs that have an explicit end but must not be consumed in parallel.").
		Fields(
			service.NewObjectField(siFieldShardedJoin,
				// TODO: V5 Remove "full-outter" and "outter"
				service.NewStringEnumField(siFieldShardedJoinType, "none", "full-outer", "outer", "full-outter", "outter").
					Description("The type of join to perform. A `full-outer` ensures that all identifiers seen in any of the input sequences are sent, and is performed by consuming all input sequences before flushing the joined results. An `outer` join consumes all input sequences but only writes data joined from the last input in the sequence, similar to a left or right outer join. With an `outer` join if an identifier appears multiple times within the final sequence input it will be flushed each time it appears. `full-outter` and `outter` have been deprecated in favour of `full-outer` and `outer`.").
					Default("none"),
				service.NewStringField(siFieldShardedJoinIDPath).
					Description("A [dot path](/docs/configuration/field_paths) that points to a common field within messages of each fragmented data set and can be used to join them. Messages that are not structured or are missing this field will be dropped. This field must be set in order to enable joins.").
					Default(""),
				service.NewIntField(siFieldShardedJoinIterations).
					Description("The total number of iterations (shards), increasing this number will increase the overall time taken to process the data, but reduces the memory used in the process. The real memory usage required is significantly higher than the real size of the data and therefore the number of iterations should be at least an order of magnitude higher than the available memory divided by the overall size of the dataset.").
					Default(1),
				service.NewStringEnumField(siFieldShardedJoinMergeStrategy, "array", "replace", "keep").
					Description("The chosen strategy to use when a data join would otherwise result in a collision of field values. The strategy `array` means non-array colliding values are placed into an array and colliding arrays are merged. The strategy `replace` replaces old values with new values. The strategy `keep` keeps the old value.").
					Default("array"),
			).
				Description(`EXPERIMENTAL: Provides a way to perform outer joins of arbitrarily structured and unordered data resulting from the input sequence, even when the overall size of the data surpasses the memory available on the machine.

When configured the sequence of inputs will be consumed one or more times according to the number of iterations, and when more than one iteration is specified each iteration will process an entirely different set of messages by sharding them by the ID field. Increasing the number of iterations reduces the memory consumption at the cost of needing to fully parse the data each time.

Each message must be structured (JSON or otherwise processed into a structured form) and the fields will be aggregated with those of other messages sharing the ID. At the end of each iteration the joined messages are flushed downstream before the next iteration begins, hence keeping memory usage limited.`).
				Version("1.0.0").
				Advanced(),
			service.NewInputListField(siFieldInputs).
				Description("An array of inputs to read from sequentially."),
		).
		Example(
			"End of Stream Message",
			"A common use case for sequence might be to generate a message at the end of our main input. With the following config once the records within `./dataset.csv` are exhausted our final payload `{\"status\":\"finished\"}` will be routed through the pipeline.",
			`
input:
  sequence:
    inputs:
      - file:
          paths: [ ./dataset.csv ]
          scanner:
            csv: {}
      - generate:
          count: 1
          mapping: 'root = {"status":"finished"}'
`,
		).
		Example(
			"Joining Data (Simple)",
			`Bento can be used to join unordered data from fragmented datasets in memory by specifying a common identifier field and a number of sharded iterations. For example, given two CSV files, the first called "main.csv", which contains rows of user data:

`+"```csv"+`
uuid,name,age
AAA,Melanie,34
BBB,Emma,28
CCC,Geri,45
`+"```"+`

And the second called "hobbies.csv" that, for each user, contains zero or more rows of hobbies:

`+"```csv"+`
uuid,hobby
CCC,pokemon go
AAA,rowing
AAA,golf
`+"```"+`

We can parse and join this data into a single dataset:

`+"```json"+`
{"uuid":"AAA","name":"Melanie","age":34,"hobbies":["rowing","golf"]}
{"uuid":"BBB","name":"Emma","age":28}
{"uuid":"CCC","name":"Geri","age":45,"hobbies":["pokemon go"]}
`+"```"+`

With the following config:`,
			`
input:
  sequence:
    sharded_join:
      type: full-outer
      id_path: uuid
      merge_strategy: array
    inputs:
      - file:
          paths:
            - ./hobbies.csv
            - ./main.csv
          scanner:
            csv: {}
`,
		).
		Example(
			"Joining Data (Advanced)",
			`In this example we are able to join unordered and fragmented data from a combination of CSV files and newline-delimited JSON documents by specifying multiple sequence inputs with their own processors for extracting the structured data.

The first file "main.csv" contains straight forward CSV data:

`+"```csv"+`
uuid,name,age
AAA,Melanie,34
BBB,Emma,28
CCC,Geri,45
`+"```"+`

And the second file called "hobbies.ndjson" contains JSON documents, one per line, that associate an identifier with an array of hobbies. However, these data objects are in a nested format:

`+"```json"+`
{"document":{"uuid":"CCC","hobbies":[{"type":"pokemon go"}]}}
{"document":{"uuid":"AAA","hobbies":[{"type":"rowing"},{"type":"golf"}]}}
`+"```"+`

And so we will want to map these into a flattened structure before the join, and then we will end up with a single dataset that looks like this:

`+"```json"+`
{"uuid":"AAA","name":"Melanie","age":34,"hobbies":["rowing","golf"]}
{"uuid":"BBB","name":"Emma","age":28}
{"uuid":"CCC","name":"Geri","age":45,"hobbies":["pokemon go"]}
`+"```"+`

With the following config:`,
			`
input:
  sequence:
    sharded_join:
      type: full-outer
      id_path: uuid
      iterations: 10
      merge_strategy: array
    inputs:
      - file:
          paths: [ ./main.csv ]
          scanner:
            csv: {}
      - file:
          paths: [ ./hobbies.ndjson ]
          scanner:
            lines: {}
        processors:
          - mapping: |
              root.uuid = this.document.uuid
              root.hobbies = this.document.hobbies.map_each(this.type)
`,
		)
}

func init() {
	err := service.RegisterBatchInput("sequence", sequenceInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newSequenceInputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalInput(i), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type joinedMessage struct {
	metadata map[string]any
	fields   *gabs.Container
}

func (j *joinedMessage) ToMsg() message.Batch {
	part := message.NewPart(nil)
	part.SetStructuredMut(message.CopyJSON(j.fields.Data()))
	for k, v := range j.metadata {
		part.MetaSetMut(k, v)
	}
	msg := message.Batch{part}
	return msg
}

type messageJoinerCollisionFn func(dest, source any) any

func getMessageJoinerCollisionFn(name string) (messageJoinerCollisionFn, error) {
	switch name {
	case "array":
		return func(dest, source any) any {
			destArr, destIsArray := dest.([]any)
			sourceArr, sourceIsArray := source.([]any)
			if destIsArray {
				if sourceIsArray {
					return append(destArr, sourceArr...)
				}
				return append(destArr, source)
			}
			if sourceIsArray {
				return append(append([]any{}, dest), sourceArr...)
			}
			return []any{dest, source}
		}, nil
	case "replace":
		return func(dest, source any) any {
			return source
		}, nil
	case "keep":
		return func(dest, source any) any {
			return dest
		}, nil
	}
	return nil, fmt.Errorf("merge strategy '%v' was not recognised", name)
}

type messageJoiner struct {
	currentIteration int
	totalIterations  int
	idPath           string
	messages         map[string]*joinedMessage
	collisionFn      messageJoinerCollisionFn
	flushOnLast      bool
}

func (m *messageJoiner) Add(msg message.Batch, lastInSequence bool, fn func(msg message.Batch)) {
	if m.messages == nil {
		m.messages = map[string]*joinedMessage{}
	}

	_ = msg.Iter(func(i int, p *message.Part) error {
		var incomingObj map[string]any
		if jData, err := p.AsStructuredMut(); err == nil {
			incomingObj, _ = jData.(map[string]any)
		}
		if incomingObj == nil {
			// Messages that aren't structured objects are dropped.
			// TODO: Propagate errors?
			return nil
		}

		gIncoming := gabs.Wrap(incomingObj)
		id, _ := gIncoming.Path(m.idPath).Data().(string)
		if id == "" {
			// TODO: Propagate errors?
			return nil
		}

		// Drop all messages that aren't within our current shard.
		if int(xxhash.ChecksumString64(id)%uint64(m.totalIterations)) != m.currentIteration {
			return nil
		}

		meta := map[string]any{}
		_ = p.MetaIterMut(func(k string, v any) error {
			meta[k] = v
			return nil
		})

		jObj := m.messages[id]
		if jObj == nil {
			jObj = &joinedMessage{
				fields:   gIncoming,
				metadata: meta,
			}
			m.messages[id] = jObj

			if m.flushOnLast && lastInSequence {
				fn(jObj.ToMsg())
			}
			return nil
		}

		_ = gIncoming.Delete(m.idPath)
		_ = jObj.fields.MergeFn(gIncoming, m.collisionFn)

		_ = p.MetaIterMut(func(k string, v any) error {
			jObj.metadata[k] = v
			return nil
		})

		if m.flushOnLast && lastInSequence {
			fn(jObj.ToMsg())
		}
		return nil
	})
}

func (m *messageJoiner) GetIteration() (int, bool) {
	return m.currentIteration, m.currentIteration == (m.totalIterations - 1)
}

func (m *messageJoiner) Empty(fn func(message.Batch)) bool {
	for k, v := range m.messages {
		if !m.flushOnLast {
			msg := v.ToMsg()
			fn(msg)
		}
		delete(m.messages, k)
	}
	m.currentIteration++
	return m.currentIteration >= m.totalIterations
}

//------------------------------------------------------------------------------

type sequenceInput struct {
	targetMut sync.Mutex
	target    input.Streamed
	remaining []sequenceTarget
	spent     []sequenceTarget

	joiner *messageJoiner

	log *service.Logger

	transactions chan message.Transaction

	shutSig *shutdown.Signaller
}

type sequenceTarget struct {
	index  int
	config *service.ParsedConfig
}

func newSequenceInputFromParsed(conf *service.ParsedConfig, res *service.Resources) (input.Streamed, error) {
	pInputConfs, err := conf.FieldAnyList(siFieldInputs)
	if err != nil {
		return nil, err
	}

	if len(pInputConfs) == 0 {
		return nil, errors.New("requires at least one child input")
	}

	targets := make([]sequenceTarget, 0, len(pInputConfs))
	for i, c := range pInputConfs {
		c := c
		targets = append(targets, sequenceTarget{
			index:  i,
			config: c,
		})
	}

	rdr := &sequenceInput{
		remaining:    targets,
		log:          res.Logger(),
		transactions: make(chan message.Transaction),
		shutSig:      shutdown.NewSignaller(),
	}

	if rdr.joiner, err = shardedConfigFromParsed(conf.Namespace(siFieldShardedJoin)); err != nil {
		return nil, fmt.Errorf("invalid sharded join config: %w", err)
	}

	if target, _, err := rdr.createNextTarget(); err != nil {
		return nil, err
	} else if target == nil {
		return nil, errors.New("failed to initialize first input")
	}

	go rdr.loop()
	return rdr, nil
}

func shardedConfigFromParsed(conf *service.ParsedConfig) (*messageJoiner, error) {
	typeStr, err := conf.FieldString(siFieldShardedJoinType)
	if err != nil {
		return nil, err
	}

	var flushOnLast bool
	switch typeStr {
	case "none":
		return nil, nil
	case "full-outer", "full-outter":
		flushOnLast = false
	case "outer", "outter":
		flushOnLast = true
	default:
		return nil, fmt.Errorf("join type '%v' was not recognized", typeStr)
	}

	idPath, _ := conf.FieldString(siFieldShardedJoinIDPath)
	if idPath == "" {
		return nil, errors.New("the id path must not be empty")
	}

	iterations, err := conf.FieldInt(siFieldShardedJoinIterations)
	if err != nil {
		return nil, err
	}
	if iterations <= 0 {
		return nil, fmt.Errorf("invalid number of iterations: %v", iterations)
	}

	mergeStrat, err := conf.FieldString(siFieldShardedJoinMergeStrategy)
	if err != nil {
		return nil, err
	}

	collisionFn, err := getMessageJoinerCollisionFn(mergeStrat)
	if err != nil {
		return nil, err
	}
	return &messageJoiner{
		totalIterations: iterations,
		idPath:          idPath,
		messages:        map[string]*joinedMessage{},
		collisionFn:     collisionFn,
		flushOnLast:     flushOnLast,
	}, nil
}

//------------------------------------------------------------------------------

func (r *sequenceInput) getTarget() (input.Streamed, bool) {
	r.targetMut.Lock()
	target := r.target
	final := len(r.remaining) == 0
	r.targetMut.Unlock()
	return target, final
}

func (r *sequenceInput) createNextTarget() (input.Streamed, bool, error) {
	var target input.Streamed
	var err error

	r.targetMut.Lock()
	r.target = nil
	if len(r.remaining) > 0 {
		next := r.remaining[0]
		if iInput, err := next.config.FieldInput(); err == nil {
			target = interop.UnwrapOwnedInput(iInput)
			r.spent = append(r.spent, next)
			r.remaining = r.remaining[1:]
		} else {
			return nil, false, fmt.Errorf("failed to initialize input index %v: %w", r.remaining[0].index, err)
		}
	}
	if target != nil {
		r.log.Debugf("Initialized sequence input %v.", len(r.spent)-1)
		r.target = target
	}
	final := len(r.remaining) == 0
	r.targetMut.Unlock()

	return target, final, err
}

func (r *sequenceInput) resetTargets() {
	r.targetMut.Lock()
	r.remaining = r.spent
	r.spent = nil
	r.targetMut.Unlock()
}

func (r *sequenceInput) dispatchJoinedMessage(wg *sync.WaitGroup, msg message.Batch) {
	resChan := make(chan error)
	tran := message.NewTransaction(msg, resChan)
	select {
	case r.transactions <- tran:
	case <-r.shutSig.HardStopChan():
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case res := <-resChan:
				if res == nil {
					return
				}
				r.log.Errorf("Failed to send joined message: %v\n", res)
			case <-r.shutSig.HardStopChan():
				return
			}
			select {
			case <-time.After(time.Second):
			case <-r.shutSig.HardStopChan():
				return
			}
			select {
			case r.transactions <- tran:
			case <-r.shutSig.HardStopChan():
				return
			}
		}
	}()
}

func (r *sequenceInput) loop() {
	shutNowCtx, done := r.shutSig.HardStopCtx(context.Background())
	defer done()

	var shardJoinWG sync.WaitGroup
	defer func() {
		shardJoinWG.Wait()
		if t, _ := r.getTarget(); t != nil {
			t.TriggerStopConsuming()
			_ = t.WaitForClose(shutNowCtx)
			t.TriggerCloseNow()
		}
		close(r.transactions)
		r.shutSig.TriggerHasStopped()
	}()

	target, finalInSequence := r.getTarget()

runLoop:
	for {
		if target == nil {
			var err error
			if target, finalInSequence, err = r.createNextTarget(); err != nil {
				r.log.Errorf("Unable to start next sequence: %v\n", err)
				select {
				case <-time.After(time.Second):
				case <-r.shutSig.SoftStopChan():
					return
				}
				continue runLoop
			}
		}
		if target == nil {
			if r.joiner != nil {
				iteration, _ := r.joiner.GetIteration()
				r.log.Debugf("Finished sharded iteration %v.", iteration)

				// Wait for pending transactions before adding more.
				shardJoinWG.Wait()

				lastIteration := r.joiner.Empty(func(msg message.Batch) {
					r.dispatchJoinedMessage(&shardJoinWG, msg)
				})
				shardJoinWG.Wait()
				if lastIteration {
					r.log.Info("Finished all sharded iterations and exhausted all sequence inputs, shutting down.")
					return
				}
				r.resetTargets()
				continue runLoop
			}

			r.log.Info("Exhausted all sequence inputs, shutting down.")
			return
		}

		var tran message.Transaction
		var open bool
		select {
		case tran, open = <-target.TransactionChan():
			if !open {
				target = nil
				continue runLoop
			}
		case <-r.shutSig.SoftStopChan():
			return
		}

		if r.joiner != nil {
			r.joiner.Add(tran.Payload, finalInSequence, func(msg message.Batch) {
				r.dispatchJoinedMessage(&shardJoinWG, msg)
			})
			if err := tran.Ack(shutNowCtx, nil); err != nil && shutNowCtx.Err() != nil {
				return
			}
		} else {
			select {
			case r.transactions <- tran:
			case <-r.shutSig.HardStopChan():
				return
			}
		}
	}
}

func (r *sequenceInput) TransactionChan() <-chan message.Transaction {
	return r.transactions
}

func (r *sequenceInput) ConnectionStatus() component.ConnectionStatuses {
	if t, _ := r.getTarget(); t != nil {
		return t.ConnectionStatus()
	}
	return nil
}

func (r *sequenceInput) TriggerStopConsuming() {
	r.shutSig.TriggerSoftStop()
}

func (r *sequenceInput) TriggerCloseNow() {
	r.shutSig.TriggerHardStop()
}

func (r *sequenceInput) WaitForClose(ctx context.Context) error {
	select {
	case <-r.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
