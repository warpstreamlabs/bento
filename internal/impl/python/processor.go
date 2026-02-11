package python

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/warpstreamlabs/bento/internal/impl/wasm/wasmpool"
	"github.com/warpstreamlabs/bento/public/service"
)

var (
	errNoWasmRuntime = errors.New("no WASM runtime found")

	// getCompliationCache allows us to compile an in-memory compilation cache once, and have it re-used between
	// processor instantiations.
	// TODO(gregfurman): Look into a wazero.NewCompilationCacheWithDir for FS persistence (likely within the ./runtime dir).
	// TODO(gregfurman): We never close the CompilationCache since it's shared between processors.
	getCompilationCache = sync.OnceValue(wazero.NewCompilationCache)
)

const (
	pythonReadySignal = "READY"

	pythonSuccessStatus = 0
	pythonFailureStatus = 1

	pythonEntrypointFile = "entrypoint.py"
	pythonExecScriptFile = "exec_script.py"
)

func init() {
	err := service.RegisterProcessor("python", pythonProcessorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newPythonProcessor(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func pythonProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Mapping").
		Summary(`
Executes a Python script against each message in a stream.
`).Description(`
This processor uses a WebAssembly (WASM) hosted Python 3.12 environment [provided by VMWare](https://github.com/vmware-labs/webassembly-language-runtimes).

Each message is passed to the script as a global variable `+"`this`"+`. The script should assign the transformed result to the global variable `+"`root`"+`.

### Libraries
A curated set of standard libraries is available. Additional modules can be specified via the `+"`imports`"+` field, provided they are available within the internal WASM runtime environment.
`).Fields(
		service.NewStringField("script").
			Description("The Python script to execute for each message."),
		service.NewStringListField("imports").
			Description("An optional list of Python modules to pre-import for the script.").
			Default([]string{}).
			Optional(),
	).Example(
		"Structured Mapping", `
If we have a stream of JSON documents containing user data, we can use Python to calculate a new field.`, `
pipeline:
  processors:
    - python:
        script: |
          root["full_name"] = f"{this['first_name']} {this['last_name']}"
          root["age_next_year"] = this["age"] + 1

# In: {"first_name": "Richie", "last_name": "Ryan", "age": 35}
# Out: {"full_name": "Richie Ryan", "age_next_year": 36}
`,
	).Example(
		"Data Filtering", `
By assigning `+"`None`"+` to `+"`root`"+`, you can effectively filter out messages based on complex logic.`, `
pipeline:
  processors:
    - python:
        script: |
          if this.pop("status", None) == "active":
              root = this

# In: {"status": "active", "region": "us-east-1"}
# Out: {"region": "us-east-1"}

# In: {"status": "inactive", "region": "af-south-1"}
# Out: null # empty
`,
	).Example(
		"External Imports", `
Using standard libraries to perform calculations.`, `
pipeline:
  processors:
    - python:
        imports: [ math ]
        script: |
          root = this
          root["rounded_value"] = math.ceil(this["value"])

# In: {"value": 3.14158}
# Out: {"value": 3.14158, "rounded_value": 3}
`,
	)
}

type pythonProcessor struct {
	script  string
	imports []string

	runtime  wazero.Runtime
	compiled wazero.CompiledModule

	pool *wasmpool.WasmModulePool[*pythonInstance]

	logger *service.Logger
}

func newPythonProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	pythonWASM, err := getPythonWasm()
	if err != nil {
		return nil, err
	}

	if len(pythonWASM) == 0 || len(pythonEntrypoint) == 0 {
		mgr.Logger().Error("cannot load in python processor without WASM runtime and entrypoint.py being set.")
		return nil, errNoWasmRuntime
	}

	ctx := context.Background()
	cache := getCompilationCache()

	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCompilationCache(cache))
	_, err = wasi_snapshot_preview1.Instantiate(ctx, r)
	if err != nil {
		return nil, err
	}

	script, err := conf.FieldString("script")
	if err != nil {
		return nil, err
	}
	imports, err := conf.FieldStringList("imports")
	if err != nil {
		return nil, err
	}

	compiled, err := r.CompileModule(ctx, pythonWASM)
	if err != nil {
		return nil, err
	}

	proc := &pythonProcessor{
		runtime:  r,
		compiled: compiled,
		imports:  imports,
		script:   script,
		logger:   mgr.Logger(),
	}

	proc.pool, err = wasmpool.NewWasmModulePool[*pythonInstance](ctx, proc.newInstance)
	if err != nil {
		return nil, err
	}

	return proc, nil
}

func (p *pythonProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	inputBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	instance, err := p.pool.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get python instance: %w", err)
	}
	defer p.pool.Put(instance)

	outputData, stderr, err := instance.runRequest(inputBytes)
	if len(stderr) > 0 {
		p.logger.Debugf("Python stderr: %s", string(stderr))
	}

	outMsg := msg.Copy()
	if err != nil {
		outMsg.SetError(err)
	} else {
		outMsg.SetBytes(outputData)
	}

	return service.MessageBatch{outMsg}, nil
}

func (p *pythonProcessor) Close(ctx context.Context) error {
	return p.runtime.Close(ctx)
}
