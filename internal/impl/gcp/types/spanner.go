package gcp

import (
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
)

// Adapted from https://github.com/apstndb/change-stream-go-sandbox/blob/0e886cbd462de560ff5c944a62f7bc427c9d03cc/types/types.go

// TimeMills fall back to UnixMicro() if it is not compatible with (*time.Time).Unmarshal()
type TimeMills time.Time

func (t *TimeMills) UnmarshalJSON(b []byte) error {
	if err := ((*time.Time)(t)).UnmarshalJSON(b); err == nil {
		return nil
	}

	i, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*t = TimeMills(time.UnixMicro(i).UTC())

	return nil
}

func (t TimeMills) MarshalJSON() ([]byte, error) {
	return time.Time(t).MarshalJSON()
}

// DataChangeRecord is compatible with spanner.Row and avroio.
type DataChangeRecord struct {
	Metadata *struct {
		ChangeStreamRecordMetadata *struct {
			NumberOfRecordsRead     int64      `json:"number_of_records_read"`
			PartitionCreatedAt      TimeMills  `json:"partition_created_at"`
			PartitionEndTimestamp   TimeMills  `json:"partition_end_timestamp"`
			PartitionRunningAt      *TimeMills `json:"partition_running_at"`
			PartitionScheduledAt    *TimeMills `json:"partition_scheduled_at"`
			PartitionStartTimestamp TimeMills  `json:"partition_start_timestamp"`
			PartitionToken          string     `json:"partition_token"`
			QueryStartedAt          TimeMills  `json:"query_started_at"`
			RecordReadAt            TimeMills  `json:"record_read_at"`
			RecordStreamEndedAt     TimeMills  `json:"record_stream_ended_at"`
			RecordStreamStartedAt   TimeMills  `json:"record_stream_started_at"`
			RecordTimestamp         TimeMills  `json:"record_timestamp"`
			TotalStreamTimeMillis   int64      `json:"total_stream_time_millis"`
		} `json:"com.google.cloud.teleport.v2.ChangeStreamRecordMetadata,omitempty"`
	} `spanner:"_" json:"metadata,omitempty"`
	CommitTimestamp                      TimeMills `spanner:"commit_timestamp" json:"commit_timestamp"`
	RecordSequence                       string    `spanner:"record_sequence" json:"record_sequence"`
	ServerTransactionId                  string    `spanner:"server_transaction_id" json:"server_transaction_id"`
	IsLastRecordInTransactionInPartition bool      `spanner:"is_last_record_in_transaction_in_partition" json:"is_last_record_in_transaction_in_partition"`
	TableName                            string    `spanner:"table_name" json:"table_name"`
	ValueCaptureType                     string    `spanner:"value_capture_type" json:"value_capture_type"`
	ColumnTypes                          []*struct {
		Name            string           `spanner:"name" json:"name"`
		Type            spanner.NullJSON `spanner:"type" json:"type"`
		IsPrimaryKey    bool             `spanner:"is_primary_key" json:"is_primary_key"`
		OrdinalPosition int64            `spanner:"ordinal_position" json:"ordinal_position"`
	} `spanner:"column_types" json:"column_types"`
	Mods                            []*Mod `spanner:"mods" json:"mods"`
	ModType                         string `spanner:"mod_type" json:"mod_type"`
	NumberOfRecordsInTransaction    int64  `spanner:"number_of_records_in_transaction" json:"number_of_records_in_transaction"`
	NumberOfPartitionsInTransaction int64  `spanner:"number_of_partitions_in_transaction" json:"number_of_partitions_in_transaction"`
	TransactionTag                  string `spanner:"transaction_tag" json:"transaction_tag"`
	IsSystemTransaction             bool   `spanner:"is_system_transaction" json:"is_system_transaction"`
}

type Mod struct {
	Keys      spanner.NullJSON `spanner:"keys" json:"keys"`
	NewValues spanner.NullJSON `spanner:"new_values" json:"new_values"`
	OldValues spanner.NullJSON `spanner:"old_values" json:"old_values"`
}

func (m *Mod) ToMap() (map[string]any, error) {
	result := make(map[string]any)

	if m.Keys.Valid {
		result["keys"] = m.Keys.Value
	}

	if m.NewValues.Valid {
		result["new_values"] = m.NewValues.Value
	}

	if m.OldValues.Valid {
		result["old_values"] = m.OldValues.Value
	}

	return result, nil
}

type HeartbeatRecord struct {
	Timestamp TimeMills `spanner:"timestamp"`
}

type ChildPartitionsRecord struct {
	RecordSequence  string    `spanner:"record_sequence"`
	StartTimestamp  TimeMills `spanner:"start_timestamp"`
	ChildPartitions []*struct {
		Token                 string   `spanner:"token"`
		ParentPartitionTokens []string `spanner:"parent_partition_tokens"`
	} `spanner:"child_partitions"`
}

type ChangeStreamRecord struct {
	DataChangeRecord      []*DataChangeRecord      `spanner:"data_change_record" json:",omitempty"`
	HeartbeatRecord       []*HeartbeatRecord       `spanner:"heartbeat_record" json:",omitempty"`
	ChildPartitionsRecord []*ChildPartitionsRecord `spanner:"child_partitions_record" json:",omitempty"`
}
