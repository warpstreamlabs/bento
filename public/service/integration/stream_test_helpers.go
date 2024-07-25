package integration

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CheckSkip marks a test to be skipped unless the integration test has been
// specifically requested using the -run flag.
func CheckSkip(t testing.TB) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^Test.*Integration.*$")
	}
}

// CheckSkipExact skips a test unless the -run flag specifically targets it.
func CheckSkipExact(t testing.TB) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || m != t.Name() {
		t.Skipf("Skipping as execution was not requested explicitly using go test -run %v", t.Name())
	}
}

// GetFreePort attempts to get a free port. This involves creating a bind and
// then immediately dropping it and so it's ever so slightly flakey.
func GetFreePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port, nil
}

// StreamTestConfigVars defines variables that will be accessed by test
// definitions when generating components through the config template. The main
// value is the id, which is generated for each test for isolation, and the port
// which is injected into the config template.
type StreamTestConfigVars struct {
	// A unique identifier for separating this test configuration from others.
	// Usually used to access a different topic, consumer group, directory, etc.
	ID string

	// A Port to use in connector URLs. Allowing tests to override this
	// potentially enables tests that check for faulty connections by bridging.
	Port string

	// General variables.
	General map[string]string
}

// StreamPreTestFn is an optional closure to be called before tests are run,
// this is an opportunity to mutate test config variables and mess with the
// environment.
type StreamPreTestFn func(t testing.TB, ctx context.Context, vars *StreamTestConfigVars)

type streamTestEnvironment struct {
	configTemplate string
	configVars     StreamTestConfigVars

	preTest StreamPreTestFn

	timeout time.Duration
	ctx     context.Context
	log     log.Modular
	stats   *metrics.Namespaced
	mgr     bundle.NewManagement

	allowDuplicateMessages bool

	// Ugly work arounds for slow connectors.
	sleepAfterInput  time.Duration
	sleepAfterOutput time.Duration
}

func newStreamTestEnvironment(t testing.TB, confTemplate string) streamTestEnvironment {
	t.Helper()

	u4, err := uuid.NewV4()
	require.NoError(t, err)

	return streamTestEnvironment{
		configTemplate: confTemplate,
		configVars: StreamTestConfigVars{
			ID: u4.String(),
			General: map[string]string{
				"MAX_IN_FLIGHT":              "1",
				"INPUT_BATCH_COUNT":          "0",
				"OUTPUT_BATCH_COUNT":         "0",
				"OUTPUT_META_EXCLUDE_PREFIX": "",
			},
		},
		timeout: time.Second * 90,
		ctx:     context.Background(),
		log:     log.Noop(),
		stats:   metrics.Noop(),
	}
}

func (e streamTestEnvironment) RenderConfig() string {
	vars := []string{
		"$ID", e.configVars.ID,
		"$PORT", e.configVars.Port,
	}
	for k, v := range e.configVars.General {
		vars = append(vars, "$"+k, v)
	}
	return strings.NewReplacer(vars...).Replace(e.configTemplate)
}

//------------------------------------------------------------------------------

// StreamTestOptFunc is an opt func for customizing the behaviour of stream
// tests, these are useful for things that are integration environment specific,
// such as the port of the service being interacted with.
type StreamTestOptFunc func(*streamTestEnvironment)

// StreamTestOptTimeout describes an optional timeout spanning the entirety of
// the test suite.
func StreamTestOptTimeout(timeout time.Duration) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.timeout = timeout
	}
}

// StreamTestOptAllowDupes specifies across all stream tests that in this
// environment we can expect duplicates and these are not considered errors.
func StreamTestOptAllowDupes() StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.allowDuplicateMessages = true
	}
}

// StreamTestOptMaxInFlight configures a maximum inflight (to be injected into
// the config template) for all tests.
func StreamTestOptMaxInFlight(n int) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.General["MAX_IN_FLIGHT"] = strconv.Itoa(n)
	}
}

// StreamTestOptLogging allows components to log with the given log level. This
// is useful for diagnosing issues.
func StreamTestOptLogging(level string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		logConf := log.NewConfig()
		logConf.LogLevel = level
		var err error
		env.log, err = log.New(os.Stdout, ifs.OS(), logConf)
		if err != nil {
			panic(err)
		}
	}
}

// StreamTestOptPort defines the port of the integration service.
func StreamTestOptPort(port string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.Port = port
	}
}

// StreamTestOptVarSet sets an arbitrary variable for the test that can
// be injected into templated configs.
func StreamTestOptVarSet(key, value string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.General[key] = value
	}
}

// StreamTestOptSleepAfterInput adds a sleep to tests after the input has been
// created.
func StreamTestOptSleepAfterInput(t time.Duration) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.sleepAfterInput = t
	}
}

// StreamTestOptSleepAfterOutput adds a sleep to tests after the output has been
// created.
func StreamTestOptSleepAfterOutput(t time.Duration) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.sleepAfterOutput = t
	}
}

// StreamTestOptPreTest adds a closure to be executed before each test.
func StreamTestOptPreTest(fn StreamPreTestFn) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.preTest = fn
	}
}

//------------------------------------------------------------------------------

type streamTestDefinitionFn func(*testing.T, *streamTestEnvironment)

// StreamTestDefinition encompasses a unit test to be executed against an
// integration environment. These tests are generic and can be run against any
// configuration containing an input and an output that are connected.
type StreamTestDefinition struct {
	fn func(*testing.T, *streamTestEnvironment)
}

// StreamTestList is a list of stream definitions that can be run with a single
// template and function args.
type StreamTestList []StreamTestDefinition

// StreamTests creates a list of tests from variadic arguments.
func StreamTests(tests ...StreamTestDefinition) StreamTestList {
	return tests
}

// Run all the tests against a config template. Tests are run in parallel.
func (i StreamTestList) Run(t *testing.T, configTemplate string, opts ...StreamTestOptFunc) {
	envs := make([]streamTestEnvironment, len(i))

	for j := range i {
		envs[j] = newStreamTestEnvironment(t, configTemplate)
		for _, opt := range opts {
			opt(&envs[j])
		}

		timeout := envs[j].timeout
		if deadline, ok := t.Deadline(); ok {
			timeout = time.Until(deadline) - (time.Second * 5)
		}

		var done func()
		envs[j].ctx, done = context.WithTimeout(envs[j].ctx, timeout)
		t.Cleanup(done)
	}

	for j, test := range i {
		if envs[j].configVars.Port == "" {
			p, err := GetFreePort()
			if err != nil {
				t.Fatal(err)
			}
			envs[j].configVars.Port = strconv.Itoa(p)
		}
		test.fn(t, &envs[j])
	}
}

//------------------------------------------------------------------------------

func namedStreamTest(name string, test streamTestDefinitionFn) StreamTestDefinition {
	return StreamTestDefinition{
		fn: func(t *testing.T, env *streamTestEnvironment) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				if env.preTest != nil {
					env.preTest(t, env.ctx, &env.configVars)
				}
				test(t, env)
			})
		},
	}
}

//------------------------------------------------------------------------------

type streamBenchDefinitionFn func(*testing.B, *streamTestEnvironment)

// StreamBenchDefinition encompasses a benchmark to be executed against an
// integration environment. These tests are generic and can be run against any
// configuration containing an input and an output that are connected.
type StreamBenchDefinition struct {
	fn streamBenchDefinitionFn
}

// StreamBenchList is a list of stream benchmark definitions that can be run
// with a single template and function args.
type StreamBenchList []StreamBenchDefinition

// StreamBenchs creates a list of benchmarks from variadic arguments.
func StreamBenchs(tests ...StreamBenchDefinition) StreamBenchList {
	return tests
}

// Run the benchmarks against a config template.
func (i StreamBenchList) Run(b *testing.B, configTemplate string, opts ...StreamTestOptFunc) {
	for _, bench := range i {
		env := newStreamTestEnvironment(b, configTemplate)
		for _, opt := range opts {
			opt(&env)
		}

		if env.preTest != nil {
			env.preTest(b, env.ctx, &env.configVars)
		}
		bench.fn(b, &env)
	}
}

func namedBench(name string, test streamBenchDefinitionFn) StreamBenchDefinition {
	return StreamBenchDefinition{
		fn: func(b *testing.B, env *streamTestEnvironment) {
			b.Run(name, func(b *testing.B) {
				test(b, env)
			})
		},
	}
}

//------------------------------------------------------------------------------

func initConnectors(
	t testing.TB,
	trans <-chan message.Transaction,
	env *streamTestEnvironment,
) (input input.Streamed, output output.Streamed) {
	t.Helper()

	out := initOutput(t, trans, env)
	in := initInput(t, env)
	return in, out
}

func initInput(t testing.TB, env *streamTestEnvironment) input.Streamed {
	t.Helper()

	node, err := docs.UnmarshalYAML([]byte(env.RenderConfig()))
	require.NoError(t, err)

	spec := docs.FieldSpecs{
		docs.FieldAnything("output", "").Optional(),
		docs.FieldInput("input", "An input to source messages from."),
	}
	spec = append(spec, manager.Spec()...)

	lints := spec.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), node)
	assert.Empty(t, lints)

	pConf, err := spec.ParsedConfigFromAny(node)
	require.NoError(t, err)

	pVal, err := pConf.FieldAny("input")
	require.NoError(t, err)

	iConf, err := input.FromAny(bundle.GlobalEnvironment, pVal)
	require.NoError(t, err)

	mConf, err := manager.FromParsed(bundle.GlobalEnvironment, pConf)
	require.NoError(t, err)

	if env.mgr == nil {
		env.mgr, err = manager.New(mConf, manager.OptSetLogger(env.log), manager.OptSetMetrics(env.stats))
		require.NoError(t, err)
	}

	input, err := env.mgr.NewInput(iConf)
	require.NoError(t, err)

	if env.sleepAfterInput > 0 {
		time.Sleep(env.sleepAfterInput)
	}

	return input
}

func initOutput(t testing.TB, trans <-chan message.Transaction, env *streamTestEnvironment) output.Streamed {
	t.Helper()

	node, err := docs.UnmarshalYAML([]byte(env.RenderConfig()))
	require.NoError(t, err)

	spec := docs.FieldSpecs{
		docs.FieldAnything("input", "").Optional(),
		docs.FieldOutput("output", "An output to source messages to."),
	}
	spec = append(spec, manager.Spec()...)

	lints := spec.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), node)
	assert.Empty(t, lints)

	pConf, err := spec.ParsedConfigFromAny(node)
	require.NoError(t, err)

	pVal, err := pConf.FieldAny("output")
	require.NoError(t, err)

	oConf, err := output.FromAny(bundle.GlobalEnvironment, pVal)
	require.NoError(t, err)

	mConf, err := manager.FromParsed(bundle.GlobalEnvironment, pConf)
	require.NoError(t, err)

	if env.mgr == nil {
		env.mgr, err = manager.New(mConf, manager.OptSetLogger(env.log), manager.OptSetMetrics(env.stats))
		require.NoError(t, err)
	}

	output, err := env.mgr.NewOutput(oConf)
	require.NoError(t, err)

	require.NoError(t, output.Consume(trans))

	if env.sleepAfterOutput > 0 {
		time.Sleep(env.sleepAfterOutput)
	}

	return output
}

func closeConnectors(t testing.TB, env *streamTestEnvironment, input input.Streamed, output output.Streamed) {
	if output != nil {
		output.TriggerCloseNow()
		require.NoError(t, output.WaitForClose(env.ctx))
	}
	if input != nil {
		input.TriggerStopConsuming()
		require.NoError(t, input.WaitForClose(env.ctx))
	}
}

func sendMessage(
	ctx context.Context,
	t testing.TB,
	tranChan chan message.Transaction,
	content string,
	metadata ...string,
) error {
	t.Helper()

	p := message.NewPart([]byte(content))
	for i := 0; i < len(metadata); i += 2 {
		p.MetaSetMut(metadata[i], metadata[i+1])
	}
	msg := message.Batch{p}
	resChan := make(chan error)

	select {
	case tranChan <- message.NewTransaction(msg, resChan):
	case <-ctx.Done():
		t.Fatal("timed out on send")
	}

	select {
	case res := <-resChan:
		return res
	case <-ctx.Done():
	}
	t.Fatal("timed out on response")
	return nil
}

func sendBatch(
	ctx context.Context,
	t testing.TB,
	tranChan chan message.Transaction,
	content []string,
) error {
	t.Helper()

	msg := message.QuickBatch(nil)
	for _, payload := range content {
		msg = append(msg, message.NewPart([]byte(payload)))
	}

	resChan := make(chan error)

	select {
	case tranChan <- message.NewTransaction(msg, resChan):
	case <-ctx.Done():
		t.Fatal("timed out on send")
	}

	select {
	case res := <-resChan:
		return res
	case <-ctx.Done():
	}

	t.Fatal("timed out on response")
	return nil
}

func receiveMessage(
	ctx context.Context,
	t testing.TB,
	tranChan <-chan message.Transaction,
	err error,
) *message.Part {
	t.Helper()

	b, ackFn := receiveBatchNoRes(ctx, t, tranChan)
	require.NoError(t, ackFn(ctx, err))
	require.Len(t, b, 1)

	msg := b.Get(0)
	require.NoError(t, msg.ErrorGet())

	return msg
}

func receiveBatch(
	ctx context.Context,
	t testing.TB,
	tranChan <-chan message.Transaction,
	err error,
) message.Batch {
	t.Helper()

	b, ackFn := receiveBatchNoRes(ctx, t, tranChan)
	require.NoError(t, ackFn(ctx, err))
	return b
}

func receiveBatchNoRes(ctx context.Context, t testing.TB, tranChan <-chan message.Transaction) (message.Batch, func(context.Context, error) error) { //nolint: gocritic // Ignore unnamedResult false positive
	t.Helper()

	var tran message.Transaction
	var open bool
	select {
	case tran, open = <-tranChan:
	case <-ctx.Done():
		t.Fatal("timed out on receive")
	}

	require.True(t, open)
	return tran.Payload, tran.Ack
}

func receiveMessageNoRes(ctx context.Context, t testing.TB, tranChan <-chan message.Transaction) (*message.Part, func(context.Context, error) error) { //nolint: gocritic // Ignore unnamedResult false positive
	t.Helper()

	b, fn := receiveBatchNoRes(ctx, t, tranChan)
	require.Len(t, b, 1)

	return b.Get(0), fn
}

func messageMatch(t testing.TB, p *message.Part, content string, metadata ...string) {
	t.Helper()

	assert.Equal(t, content, string(p.AsBytes()))

	allMetadata := map[string]string{}
	_ = p.MetaIterStr(func(k, v string) error {
		allMetadata[k] = v
		return nil
	})

	for i := 0; i < len(metadata); i += 2 {
		assert.Equal(t, metadata[i+1], p.MetaGetStr(metadata[i]), fmt.Sprintf("metadata: %v", allMetadata))
	}
}

func messagesInSet(t testing.TB, pop, allowDupes bool, b message.Batch, set map[string][]string) {
	t.Helper()

	for _, p := range b {
		metadata, exists := set[string(p.AsBytes())]
		if allowDupes && !exists {
			return
		}
		require.True(t, exists, "in set: %v, set: %v", string(p.AsBytes()), set)

		for i := 0; i < len(metadata); i += 2 {
			assert.Equal(t, metadata[i+1], p.MetaGetStr(metadata[i]))
		}

		if pop {
			delete(set, string(p.AsBytes()))
		}
	}
}
