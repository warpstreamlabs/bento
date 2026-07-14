package kafka

import (
	"fmt"

	"github.com/warpstreamlabs/bento/public/service"
)

const transactionIsolationLevelFieldName = "transaction_isolation_level"

type transactionIsolationLevel string

const (
	transactionIsolationLevelReadUncommitted transactionIsolationLevel = "read_uncommitted"
	transactionIsolationLevelReadCommitted   transactionIsolationLevel = "read_committed"
)

func newTransactionIsolationLevelField() *service.ConfigField {
	return service.NewStringAnnotatedEnumField(transactionIsolationLevelFieldName, map[string]string{
		string(transactionIsolationLevelReadUncommitted): "Consume all records, including records from aborted or open transactions.",
		string(transactionIsolationLevelReadCommitted):   "Consume only non-transactional records and records from committed transactions.",
	}).
		Description("Controls the isolation level used for Kafka fetch requests.").
		Default(string(transactionIsolationLevelReadUncommitted)).
		Advanced()
}

func transactionIsolationLevelFromConfig(conf *service.ParsedConfig) (transactionIsolationLevel, error) {
	value, err := conf.FieldString(transactionIsolationLevelFieldName)
	if err != nil {
		return "", err
	}

	level := transactionIsolationLevel(value)
	switch level {
	case transactionIsolationLevelReadUncommitted, transactionIsolationLevelReadCommitted:
		return level, nil
	default:
		return "", fmt.Errorf("invalid transaction isolation level: %q", value)
	}
}
