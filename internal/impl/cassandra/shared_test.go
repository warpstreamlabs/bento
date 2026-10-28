package cassandra

import (
	"net"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestFixedAddressTranslator(t *testing.T) {
	tr, err := newFixedAddressTranslator("127.0.0.1:9142")
	require.NoError(t, err)
	ip, port := tr.Translate(net.ParseIP("10.244.1.2"), 9042)
	assert.Equal(t, "127.0.0.1", ip.String())
	assert.Equal(t, 9142, port)

	tr, err = newFixedAddressTranslator("localhost")
	require.NoError(t, err)
	ip, port = tr.Translate(net.ParseIP("10.244.1.2"), 9042)
	assert.True(t, ip.IsLoopback())
	assert.Equal(t, 9042, port)

	tr, err = newFixedAddressTranslator("[::1]:9042")
	require.NoError(t, err)
	ip, _ = tr.Translate(net.ParseIP("10.244.1.2"), 9042)
	assert.True(t, ip.IsLoopback())

	for _, bad := range []string{"proxy:notaport", ":9042", "proxy:0", "proxy:70000"} {
		_, err = newFixedAddressTranslator(bad)
		assert.Error(t, err, bad)
	}
}

func TestClientConfTranslateAddresses(t *testing.T) {
	spec := service.NewConfigSpec().Fields(clientFields()...)

	parsed, err := spec.ParseYAML(`
addresses: [ "127.0.0.1:9042" ]
translate_addresses_to: "127.0.0.1:9042"
`, nil)
	require.NoError(t, err)
	conf, err := clientConfFromParsed(parsed)
	require.NoError(t, err)
	cluster, err := conf.Create()
	require.NoError(t, err)
	require.NotNil(t, cluster.AddressTranslator)
	ip, port := cluster.AddressTranslator.Translate(net.ParseIP("10.244.1.2"), 9042)
	assert.Equal(t, "127.0.0.1", ip.String())
	assert.Equal(t, 9042, port)

	parsed, err = spec.ParseYAML(`
addresses: [ "127.0.0.1:9042" ]
`, nil)
	require.NoError(t, err)
	conf, err = clientConfFromParsed(parsed)
	require.NoError(t, err)
	cluster, err = conf.Create()
	require.NoError(t, err)
	assert.Nil(t, cluster.AddressTranslator)

	parsed, err = spec.ParseYAML(`
addresses: [ "127.0.0.1:9042" ]
translate_addresses_to: "proxy:notaport"
`, nil)
	require.NoError(t, err)
	conf, err = clientConfFromParsed(parsed)
	require.NoError(t, err)
	_, err = conf.Create()
	require.Error(t, err)
}

func TestHostIDRoundRobinPolicy(t *testing.T) {
	// Two hosts with distinct IDs; with an address translator they would share one connect address,
	// which is what makes the stock policy's address-keyed RemoveHost drop both.
	a, b := &gocql.HostInfo{}, &gocql.HostInfo{}
	a.SetHostID("aaaa")
	b.SetHostID("bbbb")

	p := &hostIDRoundRobinPolicy{}
	p.AddHost(a)
	p.AddHost(b)
	p.AddHost(b) // idempotent
	require.Len(t, p.hosts, 2)

	p.RemoveHost(a)
	require.Len(t, p.hosts, 1)
	assert.Equal(t, "bbbb", p.hosts[0].HostID())

	p.HostDown(b)
	require.Empty(t, p.hosts)
	assert.Nil(t, p.Pick(nil)())
	p.HostUp(b)
	require.Len(t, p.hosts, 1)

	// gocql's zero host state is NodeUp, so a fresh host is picked once per pass and the pass then ends.
	next := p.Pick(nil)
	picked := next()
	require.NotNil(t, picked)
	assert.Equal(t, "bbbb", picked.Info().HostID())
	assert.Nil(t, next())

	// Translator and policy are installed together by the client config.
	spec := service.NewConfigSpec().Fields(clientFields()...)
	parsed, err := spec.ParseYAML(`
addresses: [ "127.0.0.1:9042" ]
translate_addresses_to: "127.0.0.1:9042"
`, nil)
	require.NoError(t, err)
	conf, err := clientConfFromParsed(parsed)
	require.NoError(t, err)
	cluster, err := conf.Create()
	require.NoError(t, err)
	_, ok := cluster.PoolConfig.HostSelectionPolicy.(*hostIDRoundRobinPolicy)
	assert.True(t, ok)
}
