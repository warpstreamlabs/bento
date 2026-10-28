package cassandra

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	cFieldAddresses           = "addresses"
	cFieldTLS                 = "tls"
	cFieldPassAuth            = "password_authenticator"
	cFieldPassAuthEnabled     = "enabled"
	cFieldPassAuthUsername    = "username"
	cFieldPassAuthPassword    = "password"
	cFieldDisableIHL          = "disable_initial_host_lookup"
	cFieldTranslateTo         = "translate_addresses_to"
	cFieldMaxRetries          = "max_retries"
	cFieldBackoff             = "backoff"
	cFieldBackoffInitInterval = "initial_interval"
	cFieldBackoffMaxInterval  = "max_interval"
	cFieldTimeout             = "timeout"
)

func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField(cFieldAddresses).
			Description("A list of Cassandra nodes to connect to. Multiple comma separated addresses can be specified on a single line.").
			Examples(
				[]string{"localhost:9042"},
				[]string{"foo:9042", "bar:9042"},
				[]string{"foo:9042,bar:9042"},
			),
		service.NewTLSToggledField(cFieldTLS).Advanced(),
		service.NewObjectField(cFieldPassAuth,
			service.NewBoolField(cFieldPassAuthEnabled).
				Description("Whether to use password authentication").
				Default(false),
			service.NewStringField(cFieldPassAuthUsername).
				Description("The username to authenticate as.").
				Default(""),
			service.NewStringField(cFieldPassAuthPassword).
				Description("The password to authenticate with.").
				Secret().
				Default(""),
		).
			Description("Optional configuration of Cassandra authentication parameters.").
			Advanced(),
		service.NewBoolField(cFieldDisableIHL).
			Description("If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.").
			Advanced().
			Default(false),
		service.NewStringField(cFieldTranslateTo).
			Description("Translate every node address the driver learns from the cluster (system.local, system.peers and topology events) to this `host:port`, and select hosts by host ID rather than by address. Set this when `addresses` point at a proxy, load balancer or tunnel in front of the cluster and the nodes' own addresses are not reachable from the client: without it the driver moves to those addresses as soon as it learns them and cannot reconnect once the proxied connection drops.").
			Example("cassandra-proxy:9042").
			Advanced().
			Default(""),
		service.NewIntField(cFieldMaxRetries).
			Description("The maximum number of retries before giving up on a request.").
			Advanced().
			Default(3),
		service.NewObjectField(cFieldBackoff,
			service.NewDurationField(cFieldBackoffInitInterval).
				Description("The initial period to wait between retry attempts.").
				Default("1s"),
			service.NewDurationField(cFieldBackoffMaxInterval).
				Description("The maximum period to wait between retry attempts.").
				Default("5s"),
		).
			Description("Control time intervals between retry attempts.").
			Advanced(),
		service.NewDurationField(cFieldTimeout).
			Description("The client connection timeout.").
			Default("600ms"),
	}
}

type clientConf struct {
	addresses           []string
	tlsEnabled          bool
	tlsConf             *tls.Config
	authEnabled         bool
	authUsername        string
	authPassword        string
	disableIHL          bool
	translateTo         string
	maxRetries          int
	backoffInitInterval time.Duration
	backoffMaxInterval  time.Duration
	timeout             time.Duration
}

func (c *clientConf) Create() (*gocql.ClusterConfig, error) {
	conn := gocql.NewCluster(c.addresses...)
	if c.tlsEnabled {
		conn.SslOpts = &gocql.SslOptions{
			Config: c.tlsConf,
		}
		conn.DisableInitialHostLookup = c.tlsConf.InsecureSkipVerify
	} else {
		conn.DisableInitialHostLookup = c.disableIHL
	}

	if c.authEnabled {
		conn.Authenticator = gocql.PasswordAuthenticator{
			Username: c.authUsername,
			Password: c.authPassword,
		}
	}

	if c.translateTo != "" {
		translator, err := newFixedAddressTranslator(c.translateTo)
		if err != nil {
			return nil, fmt.Errorf("field %v: %w", cFieldTranslateTo, err)
		}
		conn.AddressTranslator = translator
		// Every host now shares one address, which the stock policy cannot tell apart.
		conn.PoolConfig.HostSelectionPolicy = &hostIDRoundRobinPolicy{}
	}

	conn.RetryPolicy = &decorator{
		NumRetries: c.maxRetries,
		Min:        c.backoffInitInterval,
		Max:        c.backoffMaxInterval,
	}

	conn.Timeout = c.timeout
	return conn, nil
}

func clientConfFromParsed(conf *service.ParsedConfig) (c clientConf, err error) {
	var tmpAddresses []string
	if tmpAddresses, err = conf.FieldStringList(cFieldAddresses); err != nil {
		return
	}
	for _, a := range tmpAddresses {
		c.addresses = append(c.addresses, strings.Split(a, ",")...)
	}

	if c.tlsConf, c.tlsEnabled, err = conf.FieldTLSToggled(cFieldTLS); err != nil {
		return
	}

	{
		authConf := conf.Namespace(cFieldPassAuth)
		c.authEnabled, _ = authConf.FieldBool(cFieldPassAuthEnabled)
		c.authUsername, _ = authConf.FieldString(cFieldPassAuthUsername)
		c.authPassword, _ = authConf.FieldString(cFieldPassAuthPassword)
	}

	if c.disableIHL, err = conf.FieldBool(cFieldDisableIHL); err != nil {
		return
	}
	if c.translateTo, err = conf.FieldString(cFieldTranslateTo); err != nil {
		return
	}
	if c.maxRetries, err = conf.FieldInt(cFieldMaxRetries); err != nil {
		return
	}
	if c.backoffInitInterval, err = conf.FieldDuration(cFieldBackoff, cFieldBackoffInitInterval); err != nil {
		return
	}
	if c.backoffMaxInterval, err = conf.FieldDuration(cFieldBackoff, cFieldBackoffMaxInterval); err != nil {
		return
	}
	if c.timeout, err = conf.FieldDuration(cFieldTimeout); err != nil {
		return
	}
	return
}

// fixedAddressTranslator maps every address the driver discovers to one
// configured host:port, so a cluster that is only reachable through a proxy is
// never contacted on its nodes' own addresses.
type fixedAddressTranslator struct {
	host string
	port int
}

func newFixedAddressTranslator(hostPort string) (*fixedAddressTranslator, error) {
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		// A bare host: use the default CQL port.
		host, portStr = hostPort, "9042"
		if _, _, err := net.SplitHostPort(net.JoinHostPort(host, portStr)); err != nil {
			return nil, err
		}
	}
	if host == "" {
		return nil, fmt.Errorf("missing host in %q", hostPort)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("invalid port %q in %q", portStr, hostPort)
	}
	return &fixedAddressTranslator{host: host, port: port}, nil
}

// Translate implements gocql.AddressTranslator. The host is resolved on each
// call, which the driver makes only when it learns about a node, so the
// address follows DNS without a cache. If it cannot be resolved the original
// address is returned, which fails the same way as having no translator.
func (t *fixedAddressTranslator) Translate(addr net.IP, port int) (net.IP, int) {
	if ip := net.ParseIP(t.host); ip != nil {
		return ip, t.port
	}
	ips, err := net.LookupIP(t.host)
	if err != nil || len(ips) == 0 {
		return addr, port
	}
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			return v4, t.port
		}
	}
	return ips[0], t.port
}

// hostIDRoundRobinPolicy is gocql's round-robin policy keyed by host ID instead
// of connect address. With an address translator every host shares the proxy's
// address, and the stock policy's RemoveHost drops every host sharing the
// removed host's address: the placeholder contact point takes the real node
// with it on the first ring refresh, and the session has no host left to pick.
type hostIDRoundRobinPolicy struct {
	mu    sync.RWMutex
	hosts []*gocql.HostInfo
	next  atomic.Uint64
}

func (p *hostIDRoundRobinPolicy) IsLocal(*gocql.HostInfo) bool              { return true }
func (p *hostIDRoundRobinPolicy) KeyspaceChanged(gocql.KeyspaceUpdateEvent) {}
func (p *hostIDRoundRobinPolicy) SetPartitioner(string)                     {}
func (p *hostIDRoundRobinPolicy) Init(*gocql.Session)                       {}
func (p *hostIDRoundRobinPolicy) HostUp(host *gocql.HostInfo)               { p.AddHost(host) }
func (p *hostIDRoundRobinPolicy) HostDown(host *gocql.HostInfo)             { p.RemoveHost(host) }

// AddHost replaces the entry with the same host ID, or appends. The slice is
// never mutated in place so a snapshot taken by Pick stays valid.
func (p *hostIDRoundRobinPolicy) AddHost(host *gocql.HostInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hosts := make([]*gocql.HostInfo, 0, len(p.hosts)+1)
	for _, h := range p.hosts {
		if h.HostID() != host.HostID() {
			hosts = append(hosts, h)
		}
	}
	p.hosts = append(hosts, host)
}

func (p *hostIDRoundRobinPolicy) RemoveHost(host *gocql.HostInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hosts := make([]*gocql.HostInfo, 0, len(p.hosts))
	for _, h := range p.hosts {
		if h.HostID() != host.HostID() {
			hosts = append(hosts, h)
		}
	}
	p.hosts = hosts
}

type pickedHost struct{ host *gocql.HostInfo }

func (s pickedHost) Info() *gocql.HostInfo { return s.host }
func (s pickedHost) Mark(error)            {}

// Pick returns the up hosts starting from a rotating offset.
func (p *hostIDRoundRobinPolicy) Pick(gocql.ExecutableQuery) gocql.NextHost {
	p.mu.RLock()
	hosts := p.hosts
	p.mu.RUnlock()
	if len(hosts) == 0 {
		return func() gocql.SelectedHost { return nil }
	}
	start := int(p.next.Add(1) % uint64(len(hosts)))
	i := 0
	return func() gocql.SelectedHost {
		for i < len(hosts) {
			h := hosts[(start+i)%len(hosts)]
			i++
			if h.IsUp() {
				return pickedHost{host: h}
			}
		}
		return nil
	}
}
