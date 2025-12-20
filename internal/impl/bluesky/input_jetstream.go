package bluesky

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/public/service"
)

// Jetstream public instances
var defaultJetstreamEndpoints = []string{
	"wss://jetstream1.us-east.bsky.network/subscribe",
	"wss://jetstream2.us-east.bsky.network/subscribe",
	"wss://jetstream1.us-west.bsky.network/subscribe",
	"wss://jetstream2.us-west.bsky.network/subscribe",
}

// JetstreamEvent represents a message from the Jetstream firehose
type JetstreamEvent struct {
	Did    string `json:"did"`
	TimeUS int64  `json:"time_us"`
	Kind   string `json:"kind"` // "commit", "identity", "account"

	// For commit events
	Commit *CommitEvent `json:"commit,omitempty"`

	// For identity events
	Identity *IdentityEvent `json:"identity,omitempty"`

	// For account events
	Account *AccountEvent `json:"account,omitempty"`
}

// CommitEvent represents a repository commit (create/update/delete)
type CommitEvent struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"` // "create", "update", "delete"
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"`
	Cid        string          `json:"cid,omitempty"`
}

// IdentityEvent represents a handle/identity change
type IdentityEvent struct {
	Did    string `json:"did"`
	Handle string `json:"handle"`
	Seq    int64  `json:"seq"`
	Time   string `json:"time"`
}

// AccountEvent represents an account status change
type AccountEvent struct {
	Active bool   `json:"active"`
	Did    string `json:"did"`
	Seq    int64  `json:"seq"`
	Time   string `json:"time"`
	Status string `json:"status,omitempty"` // "takendown", "suspended", "deleted", "deactivated"
}

func jetstreamInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services", "Social").
		Summary("Connects to the Bluesky Jetstream firehose to consume real-time events from the AT Protocol network.").
		Description(`
This input connects to [Bluesky's Jetstream](https://docs.bsky.app/blog/jetstream) service, which provides a simplified,
JSON-based stream of events from the AT Protocol network. Events include posts, likes, follows, reposts, and more.

Jetstream is a lightweight alternative to the full AT Protocol firehose, making it easier to build applications that
consume real-time Bluesky data without dealing with complex CBOR/CAR binary formats.

### Event Types

The stream includes three types of events:

- **commit**: Repository changes (posts, likes, follows, blocks, etc.)
- **identity**: Handle and identity changes
- **account**: Account status changes (active, suspended, deleted)

### Custom PDS Support

While the default configuration connects to Bluesky's public Jetstream servers, you can point this input to any
AT Protocol PDS (Personal Data Server) that exposes a Jetstream-compatible endpoint. This enables:

- Reading from self-hosted PDS instances
- Connecting to alternative AT Protocol networks
- Testing against local development servers

### Cursor-Based Resumption

The input supports cursor-based resumption via an optional cache resource. When configured, the cursor (Unix
microsecond timestamp) of the last processed event is stored in the cache. On restart, the stream resumes
from that point, ensuring no events are missed.
`).
		Fields(
			service.NewStringField("endpoint").
				Description("The Jetstream WebSocket endpoint URL. Leave empty to use the default public endpoints with automatic failover.").
				Example("wss://jetstream1.us-east.bsky.network/subscribe").
				Example("wss://my-pds.example.com/xrpc/com.atproto.sync.subscribeRepos").
				Default("").
				Advanced(),
			service.NewStringListField("collections").
				Description("Filter events by collection NSID (Namespaced Identifier). Supports wildcards like `app.bsky.feed.*`. Leave empty to receive all collections.").
				Example([]string{"app.bsky.feed.post"}).
				Example([]string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"}).
				Example([]string{"app.bsky.feed.*"}).
				Default([]string{}),
			service.NewStringListField("dids").
				Description("Filter events by repository DID (Decentralized Identifier). Maximum 10,000 DIDs. Leave empty to receive events from all users.").
				Example([]string{"did:plc:z72i7hdynmk6r22z27h6tvur"}).
				Default([]string{}),
			service.NewBoolField("compress").
				Description("Enable zstd compression for reduced bandwidth. Recommended for high-volume streams.").
				Default(false).
				Advanced(),
			service.NewStringField("cache").
				Description("A cache resource for storing the cursor position. This enables resumption from the last processed event after restarts.").
				Default("").
				Advanced(),
			service.NewStringField("cache_key").
				Description("The key used to store the cursor in the cache.").
				Default("bluesky_jetstream_cursor").
				Advanced(),
			service.NewIntField("cursor_buffer_us").
				Description("When resuming, subtract this many microseconds from the stored cursor to ensure no events are missed due to timing issues. Default is 1 second (1,000,000 microseconds).").
				Default(1000000).
				Advanced(),
			service.NewAutoRetryNacksToggleField(),
		)
}

func init() {
	err := service.RegisterInput(
		"bluesky_jetstream", jetstreamInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			reader, err := newJetstreamReader(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, reader)
		},
	)
	if err != nil {
		panic(err)
	}
}

type jetstreamReader struct {
	log     *service.Logger
	shutSig *shutdown.Signaller
	mgr     *service.Resources

	checkpointer *checkpoint.Capped[int64]

	// Config
	endpoint       string
	collections    []string
	dids           []string
	compress       bool
	cache          string
	cacheKey       string
	cursorBufferUS int64

	// Connection state
	connMut    sync.Mutex
	conn       *websocket.Conn
	msgChan    chan *JetstreamEvent
	zstdReader *zstd.Decoder
}

func newJetstreamReader(conf *service.ParsedConfig, mgr *service.Resources) (*jetstreamReader, error) {
	r := &jetstreamReader{
		log:          mgr.Logger(),
		shutSig:      shutdown.NewSignaller(),
		mgr:          mgr,
		checkpointer: checkpoint.NewCapped[int64](1024),
	}
	var err error

	if r.endpoint, err = conf.FieldString("endpoint"); err != nil {
		return nil, err
	}
	if r.collections, err = conf.FieldStringList("collections"); err != nil {
		return nil, err
	}
	if r.dids, err = conf.FieldStringList("dids"); err != nil {
		return nil, err
	}
	if r.compress, err = conf.FieldBool("compress"); err != nil {
		return nil, err
	}
	if r.cache, err = conf.FieldString("cache"); err != nil {
		return nil, err
	}
	if r.cacheKey, err = conf.FieldString("cache_key"); err != nil {
		return nil, err
	}

	cursorBuffer, err := conf.FieldInt("cursor_buffer_us")
	if err != nil {
		return nil, err
	}
	r.cursorBufferUS = int64(cursorBuffer)

	// Validate configuration
	if len(r.collections) > 100 {
		return nil, errors.New("maximum of 100 collections allowed")
	}
	if len(r.dids) > 10000 {
		return nil, errors.New("maximum of 10,000 DIDs allowed")
	}

	return r, nil
}

func (r *jetstreamReader) buildURL(cursor int64) (string, error) {
	var baseURL string
	if r.endpoint != "" {
		baseURL = r.endpoint
	} else {
		// Use first public endpoint (could add failover logic later)
		baseURL = defaultJetstreamEndpoints[0]
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	q := u.Query()

	// Add collection filters
	for _, collection := range r.collections {
		q.Add("wantedCollections", collection)
	}

	// Add DID filters
	for _, did := range r.dids {
		q.Add("wantedDids", did)
	}

	// Add compression flag
	if r.compress {
		q.Set("compress", "true")
	}

	// Add cursor for resumption
	if cursor > 0 {
		q.Set("cursor", strconv.FormatInt(cursor, 10))
	}

	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (r *jetstreamReader) getCursor(ctx context.Context) (int64, error) {
	if r.cache == "" {
		return 0, nil
	}

	var cursor int64
	var cacheErr error

	err := r.mgr.AccessCache(ctx, r.cache, func(c service.Cache) {
		var cursorBytes []byte
		if cursorBytes, cacheErr = c.Get(ctx, r.cacheKey); errors.Is(cacheErr, service.ErrKeyNotFound) {
			cacheErr = nil
			return
		}
		if cacheErr != nil {
			return
		}
		cursor, cacheErr = strconv.ParseInt(string(cursorBytes), 10, 64)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to access cache: %w", err)
	}
	if cacheErr != nil {
		return 0, fmt.Errorf("failed to read cursor from cache: %w", cacheErr)
	}

	// Apply buffer to ensure no events are missed
	if cursor > r.cursorBufferUS {
		cursor -= r.cursorBufferUS
	}

	return cursor, nil
}

func (r *jetstreamReader) saveCursor(ctx context.Context, cursor int64) error {
	if r.cache == "" {
		return nil
	}

	var setErr error
	if err := r.mgr.AccessCache(ctx, r.cache, func(c service.Cache) {
		setErr = c.Set(ctx, r.cacheKey, []byte(strconv.FormatInt(cursor, 10)), nil)
	}); err != nil {
		return err
	}
	return setErr
}

func (r *jetstreamReader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.msgChan != nil {
		return nil
	}

	// Get cursor for resumption
	cursor, err := r.getCursor(ctx)
	if err != nil {
		r.log.Warnf("Failed to get cursor from cache, starting from live: %v", err)
		cursor = 0
	}

	if cursor > 0 {
		r.log.Infof("Resuming from cursor: %d (approximately %s ago)",
			cursor, time.Since(time.UnixMicro(cursor)).Round(time.Second))
	}

	// Build WebSocket URL
	wsURL, err := r.buildURL(cursor)
	if err != nil {
		return err
	}

	r.log.Debugf("Connecting to Jetstream: %s", wsURL)

	// Connect with custom headers for compression
	dialer := websocket.DefaultDialer
	headers := make(map[string][]string)
	if r.compress {
		headers["Socket-Encoding"] = []string{"zstd"}
	}

	conn, resp, err := dialer.DialContext(ctx, wsURL, headers)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("failed to connect to Jetstream: %w", err)
	}

	// Initialize zstd decoder if compression is enabled
	if r.compress {
		r.zstdReader, err = zstd.NewReader(nil)
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to create zstd decoder: %w", err)
		}
	}

	msgChan := make(chan *JetstreamEvent, 1024)

	go func() {
		defer func() {
			conn.Close()
			if r.zstdReader != nil {
				r.zstdReader.Close()
			}
			r.shutSig.TriggerHasStopped()
		}()

		for {
			if r.shutSig.IsSoftStopSignalled() {
				return
			}

			_, data, err := conn.ReadMessage()
			if err != nil {
				if !r.shutSig.IsSoftStopSignalled() {
					r.log.Errorf("WebSocket read error: %v", err)
				}
				return
			}

			// Decompress if needed
			if r.compress && r.zstdReader != nil {
				decompressed, err := r.decompressMessage(data)
				if err != nil {
					r.log.Errorf("Failed to decompress message: %v", err)
					continue
				}
				data = decompressed
			}

			var event JetstreamEvent
			if err := json.Unmarshal(data, &event); err != nil {
				r.log.Errorf("Failed to parse Jetstream event: %v", err)
				continue
			}

			select {
			case msgChan <- &event:
			case <-r.shutSig.SoftStopChan():
				return
			}
		}
	}()

	r.conn = conn
	r.msgChan = msgChan
	r.log.Infof("Connected to Bluesky Jetstream")
	return nil
}

func (r *jetstreamReader) decompressMessage(data []byte) ([]byte, error) {
	if r.zstdReader == nil {
		return nil, errors.New("zstd reader not initialized")
	}

	err := r.zstdReader.Reset(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	return io.ReadAll(r.zstdReader)
}

func (r *jetstreamReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	r.connMut.Lock()
	msgChan := r.msgChan
	r.connMut.Unlock()

	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	var event *JetstreamEvent
	select {
	case event = <-msgChan:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	// Marshal event to JSON for the message payload
	jBytes, err := json.Marshal(event)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Track this event's cursor for checkpointing
	release, err := r.checkpointer.Track(ctx, event.TimeUS, 1)
	if err != nil {
		return nil, nil, err
	}

	msg := service.NewMessage(jBytes)

	// Add metadata for downstream processing
	msg.MetaSet("bluesky_did", event.Did)
	msg.MetaSet("bluesky_kind", event.Kind)
	msg.MetaSet("bluesky_time_us", strconv.FormatInt(event.TimeUS, 10))

	if event.Commit != nil {
		msg.MetaSet("bluesky_collection", event.Commit.Collection)
		msg.MetaSet("bluesky_operation", event.Commit.Operation)
		msg.MetaSet("bluesky_rkey", event.Commit.Rkey)
	}

	return msg, func(ctx context.Context, err error) error {
		highestCursor := release()
		if highestCursor == nil {
			return nil
		}
		return r.saveCursor(ctx, *highestCursor)
	}, nil
}

func (r *jetstreamReader) Close(ctx context.Context) error {
	go func() {
		r.shutSig.TriggerSoftStop()
		r.connMut.Lock()
		if r.msgChan == nil {
			r.shutSig.TriggerHasStopped()
		}
		r.connMut.Unlock()
	}()

	select {
	case <-r.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
