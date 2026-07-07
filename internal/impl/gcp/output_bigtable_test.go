package gcp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

// NOTE(gregfurman): The underlying BigTable client checks for the presence of this BIGTABLE_EMULATOR_HOST env var.

const envBigTableEmulatorHost = "BIGTABLE_EMULATOR_HOST"

func TestBigTableOutput(t *testing.T) {
	server, err := setupBigTableEmulator(t)
	require.NoError(t, err)
	defer server.Close()

	tableID := "test-table"
	columnFamily := "test-family"
	colExpr := `${! meta("column") }`
	rowKeyExpr := `${! meta("column") }#${! counter() }`

	tableConf := bigtable.TableConf{
		TableID: tableID,
		ColumnFamilies: map[string]bigtable.Family{
			columnFamily: {
				GCPolicy: bigtable.NoGcPolicy(),
				ValueType: bigtable.StringType{
					Encoding: bigtable.StringUtf8BytesEncoding{},
				},
			},
		},
	}
	err = setupTableInstance(t.Context(), &tableConf)
	require.NoError(t, err)

	spec := gcpBigTableOutputSpec()
	parsedConf, err := spec.ParseYAML(fmt.Sprintf(`
project: %s
instance: %s
table: %s
column: %s
family: %s
row_key: %s
`, projectID, instanceID, tableID, colExpr, columnFamily, rowKeyExpr), nil)
	require.NoError(t, err)

	conf, err := gcpBigTableOutputConfigFromParsed(parsedConf)
	require.NoError(t, err)

	output, err := newGCPBigTableOutput(conf)
	require.NoError(t, err)

	err = output.Connect(t.Context())
	require.NoError(t, err)

	var inBatch service.MessageBatch
	var wantValues []string
	for ind := range 10 {
		val := fmt.Sprintf("value-%d", ind)
		wantValues = append(wantValues, val)

		msg := service.NewMessage([]byte(val))
		col := "col_odd"
		if ind%2 == 0 {
			col = "col_even"
		}
		msg.MetaSetMut("column", col)
		inBatch = append(inBatch, msg)
	}

	err = output.WriteBatch(t.Context(), inBatch)
	require.NoError(t, err)

	rows, err := readRows(t.Context(), "col_", columnFamily, tableID)
	require.NoError(t, err)
	require.Len(t, rows, 10)

	var gotValues []string
	for _, row := range rows {
		cells := row[columnFamily]
		require.NotEmpty(t, cells, "expected cells for row %s", row.Key())
		for _, cell := range cells {
			gotValues = append(gotValues, string(cell.Value))
		}
	}

	require.ElementsMatch(t, wantValues, gotValues)

	err = output.Close(t.Context())
	require.NoError(t, err)
}

func TestBigTableOutputErrors(t *testing.T) {
	tests := []struct {
		name       string
		setupTable bool
		writeBatch func(output *gcpBigTableOutput) error
		wantErr    bool
	}{
		{
			name:       "write after context cancelled",
			setupTable: true,
			writeBatch: func(output *gcpBigTableOutput) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return output.WriteBatch(ctx, service.MessageBatch{service.NewMessage([]byte("value"))})
			},
			wantErr: true,
		},
		{
			name:       "write after close",
			setupTable: true,
			writeBatch: func(output *gcpBigTableOutput) error {
				require.NoError(t, output.client.Close())
				return output.WriteBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte("value"))})
			},
			wantErr: true,
		},
		{
			name:       "write to non-existent table",
			setupTable: false,
			writeBatch: func(output *gcpBigTableOutput) error {
				return output.WriteBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte("value"))})
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, err := setupBigTableEmulator(t)
			require.NoError(t, err)
			defer server.Close()

			tableID := fmt.Sprintf("test-table-%s", strings.ReplaceAll(tc.name, " ", "-"))
			columnFamily := "test-family"

			if tc.setupTable {
				err = setupTableInstance(t.Context(), &bigtable.TableConf{
					TableID: tableID,
					ColumnFamilies: map[string]bigtable.Family{
						columnFamily: {GCPolicy: bigtable.NoGcPolicy()},
					},
				})
				require.NoError(t, err)
			}

			spec := gcpBigTableOutputSpec()
			parsedConf, err := spec.ParseYAML(fmt.Sprintf(`
project: %s
instance: %s
table: %s
column: col
family: %s
row_key: key
`, projectID, instanceID, tableID, columnFamily), nil)
			require.NoError(t, err)

			conf, err := gcpBigTableOutputConfigFromParsed(parsedConf)
			require.NoError(t, err)

			output, err := newGCPBigTableOutput(conf)
			require.NoError(t, err)

			err = output.Connect(t.Context())
			require.NoError(t, err)

			err = tc.writeBatch(output)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBigTableOutputTimestamp(t *testing.T) {
	staticTime := time.Date(2024, time.June, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		timestamp string
		prepMsg   func(msg *service.Message)
		checkTS   func(t *testing.T, ts bigtable.Timestamp)
		wantErr   bool
	}{
		{
			name:      "static unix timestamp",
			timestamp: fmt.Sprintf("'%d'", staticTime.Unix()),
			checkTS: func(t *testing.T, ts bigtable.Timestamp) {
				require.Equal(t, bigtable.Time(staticTime), ts)
			},
		},
		{
			name:      "timestamp from metadata",
			timestamp: `'metadata("ts")'`,
			prepMsg: func(msg *service.Message) {
				msg.MetaSetMut("ts", staticTime.Unix())
			},
			checkTS: func(t *testing.T, ts bigtable.Timestamp) {
				require.Equal(t, bigtable.Time(staticTime), ts)
			},
		},
		{
			name:      "defaults to wall clock when unset",
			timestamp: "",
			checkTS: func(t *testing.T, ts bigtable.Timestamp) {
				require.WithinDuration(t, time.Now(), ts.Time(), time.Minute)
			},
		},
		{
			name:      "defaults to server time when -1",
			timestamp: "-1",
			checkTS: func(t *testing.T, ts bigtable.Timestamp) {
				require.WithinDuration(t, time.Now(), ts.Time(), time.Minute)
			},
		},
		{
			name:      "invalid timestamp value",
			timestamp: `'"not-a-timestamp"'`,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, err := setupBigTableEmulator(t)
			require.NoError(t, err)
			defer server.Close()

			tableID := fmt.Sprintf("ts-table-%s", strings.ReplaceAll(tc.name, " ", "-"))
			columnFamily := "test-family"

			err = setupTableInstance(t.Context(), &bigtable.TableConf{
				TableID: tableID,
				ColumnFamilies: map[string]bigtable.Family{
					columnFamily: {GCPolicy: bigtable.NoGcPolicy()},
				},
			})
			require.NoError(t, err)

			spec := gcpBigTableOutputSpec()
			parsedConf, err := spec.ParseYAML(fmt.Sprintf(`
project: %s
instance: %s
table: %s
column: col
family: %s
row_key: key
timestamp: %s
`, projectID, instanceID, tableID, columnFamily, tc.timestamp), nil)
			require.NoError(t, err)

			conf, err := gcpBigTableOutputConfigFromParsed(parsedConf)
			require.NoError(t, err)

			output, err := newGCPBigTableOutput(conf)
			require.NoError(t, err)

			err = output.Connect(t.Context())
			require.NoError(t, err)
			defer output.Close(t.Context())

			msg := service.NewMessage([]byte("value"))
			if tc.prepMsg != nil {
				tc.prepMsg(msg)
			}

			err = output.WriteBatch(t.Context(), service.MessageBatch{msg})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			rows, err := readRows(t.Context(), "key", columnFamily, tableID)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			cells := rows[0][columnFamily]
			require.Len(t, cells, 1)

			tc.checkTS(t, cells[0].Timestamp)
		})
	}
}

//------------------------------------------------------------------------------

func setupBigTableEmulator(t *testing.T) (*bttest.Server, error) {
	t.Helper()

	port, err := integration.GetFreePort()
	if err != nil {
		return nil, err
	}

	server, err := bttest.NewServer(fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}

	t.Setenv(envBigTableEmulatorHost, server.Addr)

	return server, nil
}

func setupTableInstance(ctx context.Context, conf *bigtable.TableConf) error {
	adminClient, err := bigtable.NewAdminClient(ctx, projectID, instanceID)
	if err != nil {
		return err
	}

	return adminClient.CreateTableFromConf(ctx, conf)
}

func readRows(ctx context.Context, col, family, tableID string) ([]bigtable.Row, error) {
	clientConf := bigtable.ClientConfig{
		MetricsProvider: bigtable.NoopMetricsProvider{},
	}
	client, err := bigtable.NewClientWithConfig(ctx, projectID, instanceID, clientConf)
	if err != nil {
		return nil, err
	}

	tbl := client.Open(tableID)

	var rows []bigtable.Row
	err = tbl.ReadRows(ctx, bigtable.PrefixRange(col), func(row bigtable.Row) bool {
		rows = append(rows, row)
		return true
	}, bigtable.RowFilter(bigtable.ChainFilters(
		bigtable.FamilyFilter(family),
		bigtable.LatestNFilter(1),
	)),
	)

	if err != nil {
		return nil, err
	}

	return rows, nil
}
