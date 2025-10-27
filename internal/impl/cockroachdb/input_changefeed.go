package crdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Jeffail/gabs/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/Jeffail/checkpoint"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/lib/pq"
)

var sampleString = `{
	"primary_key": "[\"1a7ff641-3e3b-47ee-94fe-a0cadb56cd8f\", 2]", // stringifed JSON array
	"row": "{\"after\": {\"k\": \"1a7ff641-3e3b-47ee-94fe-a0cadb56cd8f\", \"v\": 2}, \"updated\": \"1637953249519902405.0000000000\"}", // stringified JSON object
	"table": "strm_2"
}`

func crdbChangefeedInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration").
		Summary(fmt.Sprintf("Listens to a [CockroachDB Core Changefeed](https://www.cockroachlabs.com/docs/stable/changefeed-examples) and creates a message for each row received. Each message is a json object looking like: \n```json\n%s\n```", sampleString)).
		Description("This input will continue to listen to the changefeed until shutdown. A backfill of the full current state of the table will be delivered upon each run unless a cache is configured for storing cursor timestamps, as this is how Bento keeps track as to which changes have been successfully delivered.\n\nNote: You must have `SET CLUSTER SETTING kv.rangefeed.enabled = true;` on your CRDB cluster, for more information refer to [the official CockroachDB documentation.](https://www.cockroachlabs.com/docs/stable/changefeed-examples?filters=core)").
		Fields(
			service.NewStringField("dsn").
				Description(`A Data Source Name to identify the target database.`).
				Example("postgres://user:password@example.com:26257/defaultdb?sslmode=require"),
			service.NewTLSField("tls"),
			service.NewStringListField("tables").
				Description("CSV of tables to be included in the changefeed").
				Example([]string{"table1", "table2"}),
			service.NewStringField("cursor_cache").
				Description("A [cache resource](https://warpstreamlabs.github.io/bento/docs/components/caches/about) to use for storing the current latest cursor that has been successfully delivered, this allows Bento to continue from that cursor upon restart, rather than consume the entire state of the table.").
				Optional(),
			service.NewStringListField("options").
				Description("A list of options to be included in the changefeed (WITH X, Y...).\n**NOTE: Both the CURSOR option and UPDATED will be ignored from these options when a `cursor_cache` is specified, as they are set explicitly by Bento in this case.**").
				Example([]string{`virtual_columns="omitted"`}).
				Advanced().
				Optional(),
			service.NewAutoRetryNacksToggleField(),
		)
}

type crdbChangefeedResult struct {
	values []any
	err    error
}

type crdbChangefeedInput struct {
	statement          string
	cursorCache        string
	cursorCheckpointer *checkpoint.Capped[string]

	pgConfig *pgxpool.Config
	pgPool   *pgxpool.Pool
	rows     pgx.Rows
	dbMut    sync.Mutex

	res     *service.Resources
	logger  *service.Logger
	shutSig *shutdown.Signaller

	resultChan chan crdbChangefeedResult
}

const cursorCacheKey = "crdb_changefeed_cursor"

func newCRDBChangefeedInputFromConfig(conf *service.ParsedConfig, res *service.Resources) (*crdbChangefeedInput, error) {
	c := &crdbChangefeedInput{
		cursorCheckpointer: checkpoint.NewCapped[string](1024), // TODO: Configure this?
		res:                res,
		logger:             res.Logger(),
		shutSig:            shutdown.NewSignaller(),
	}

	dsn, err := conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}

	if c.pgConfig, err = pgxpool.ParseConfig(dsn); err != nil {
		return nil, err
	}

	if c.pgConfig.ConnConfig.TLSConfig, err = conf.FieldTLS("tls"); err != nil {
		return nil, err
	}

	c.cursorCache, _ = conf.FieldString("cursor_cache")

	// Setup the query
	tables, err := conf.FieldStringList("tables")
	if err != nil {
		return nil, err
	}

	tmpOptions, _ := conf.FieldStringList("options")

	var options []string
	if c.cursorCache == "" {
		options = tmpOptions
	} else {
		for _, o := range tmpOptions {
			if strings.HasPrefix(strings.ToLower(o), "updated") {
				continue
			}
			if strings.HasPrefix(strings.ToLower(o), "cursor") {
				continue
			}
			options = append(options, o)
		}
		options = append(options, "UPDATED")
		if err := res.AccessCache(context.Background(), c.cursorCache, func(c service.Cache) {
			cursorBytes, cErr := c.Get(context.Background(), cursorCacheKey)
			if cErr != nil {
				if !errors.Is(cErr, service.ErrKeyNotFound) {
					res.Logger().With("error", cErr.Error()).Error("Failed to obtain cursor cache item.")
				}
				return
			}
			options = append(options, `CURSOR="`+string(cursorBytes)+`"`)
		}); err != nil {
			res.Logger().With("error", err.Error()).Error("Failed to access cursor cache.")
		}
	}

	changeFeedOptions := ""
	if len(options) > 0 {
		changeFeedOptions = " WITH " + strings.Join(options, ", ")
	}

	c.statement = fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s%s", strings.Join(tables, ", "), changeFeedOptions)
	res.Logger().Debug("Creating changefeed: " + c.statement)

	return c, nil
}

func init() {
	err := service.RegisterInput(
		"cockroachdb_changefeed", crdbChangefeedInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newCRDBChangefeedInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
	if err != nil {
		panic(err)
	}
}

func (c *crdbChangefeedInput) Connect(ctx context.Context) (err error) {
	c.dbMut.Lock()
	defer c.dbMut.Unlock()

	if c.resultChan != nil {
		return
	}

	if c.rows != nil {
		return
	}

	if c.shutSig.IsSoftStopSignalled() {
		return service.ErrEndOfInput
	}

	if c.pgPool == nil {
		if c.pgPool, err = pgxpool.ConnectConfig(ctx, c.pgConfig); err != nil {
			return
		}
		defer func() {
			if err != nil {
				c.pgPool.Close()
				c.pgPool = nil
			}
		}()
	}

	c.logger.Debug(fmt.Sprintf("Running query '%s'", c.statement))
	c.rows, err = c.pgPool.Query(ctx, c.statement)
	c.resultChan = make(chan crdbChangefeedResult, 1)

	go c.loop()
	return
}

func (c *crdbChangefeedInput) closeConnection() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Errorf("Recovered connection close panic: %v", r)
		}
	}()

	c.dbMut.Lock()
	defer c.dbMut.Unlock()

	if c.rows != nil {
		err := c.rows.Err()
		if err != nil {
			c.logger.With("err", err).Warn("unexpected error from cockroachdb before closing")
		}

		c.rows.Close()
		c.rows = nil
	}
	if c.pgPool != nil {
		c.pgPool.Close()
		c.pgPool = nil
	}
	if c.resultChan != nil {
		close(c.resultChan)
		c.resultChan = nil
	}
}

func (c *crdbChangefeedInput) loop() {
	// teardown & close pgx and channel attributes
	defer c.closeConnection()

	defer c.shutSig.TriggerHasStopped()
	for {
		record := crdbChangefeedResult{}
		if !c.rows.Next() {
			record.err = c.rows.Err()
		} else {
			record.values, record.err = c.rows.Values()
		}

		select {
		// This allows us to buffer the input to only read 1 record at a time
		case c.resultChan <- record:
		case <-c.shutSig.SoftStopChan():
			return
		}
	}
}

func (c *crdbChangefeedInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	c.dbMut.Lock()
	resultChan := c.resultChan
	c.dbMut.Unlock()

	if resultChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	result := crdbChangefeedResult{}
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case res, ok := <-resultChan:
		if !ok {
			return nil, nil, service.ErrNotConnected
		}
		result = res
	}

	values, err := result.values, result.err
	if result.err != nil {
		return nil, nil, fmt.Errorf("row values: %w", err)
	}

	if values == nil {
		return nil, nil, service.ErrNotConnected
	}

	var cursorReleaseFn func() *string

	rowBytes := values[2].([]byte)
	if gObj, err := gabs.ParseJSON(rowBytes); err == nil {
		if cursorTimestamp, _ := gObj.S("updated").Data().(string); cursorTimestamp != "" {
			cursorReleaseFn, _ = c.cursorCheckpointer.Track(ctx, cursorTimestamp, 1)
		}
	}

	// Construct the new JSON
	var jsonBytes []byte
	if jsonBytes, err = json.Marshal(map[string]string{
		"table":       values[0].(string),
		"primary_key": string(values[1].([]byte)), // Stringified JSON (Array)
		"row":         string(rowBytes),           // Stringified JSON (Object)
	}); err != nil {
		return nil, nil, err
	}

	msg := service.NewMessage(jsonBytes)
	return msg, func(ctx context.Context, err error) (cErr error) {
		if cursorReleaseFn == nil {
			return nil
		}
		cursorTimestamp := cursorReleaseFn()
		if cursorTimestamp == nil {
			return nil
		}
		if err := c.res.AccessCache(ctx, c.cursorCache, func(c service.Cache) {
			cErr = c.Set(ctx, cursorCacheKey, []byte(*cursorTimestamp), nil)
		}); err != nil {
			return err
		}
		return
	}, nil
}

func (c *crdbChangefeedInput) Close(ctx context.Context) error {
	c.dbMut.Lock()
	resultChan := c.resultChan
	c.dbMut.Unlock()

	if resultChan == nil {
		return nil
	}

	c.shutSig.TriggerHardStop()
	select {
	case <-c.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
