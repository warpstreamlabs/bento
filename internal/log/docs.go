package log

import (
	"bytes"
	"text/template"

	"github.com/warpstreamlabs/bento/internal/docs"

	_ "embed"
)

// Spec returns a field spec for the logger configuration fields.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(fieldLogLevel, "Set the minimum severity level for emitting logs.").HasOptions(
			"OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL", "NONE",
		).HasDefault("INFO").LinterFunc(nil),
		docs.FieldString(fieldFormat, "Set the format of emitted logs.").HasOptions("json", "logfmt").HasDefault("logfmt"),
		docs.FieldBool(fieldAddTimeStamp, "Whether to include timestamps in logs.").HasDefault(false),
		docs.FieldString(fieldLevelName, "The name of the level field added to logs when the `format` is `json`.").HasDefault("level"),
		docs.FieldString(fieldTimestampName, "The name of the timestamp field added to logs when `add_timestamp` is set to `true` and the `format` is `json`.").HasDefault("time"),
		docs.FieldString(fieldMessageName, "The name of the message field added to logs when the `format` is `json`.").HasDefault("msg"),
		docs.FieldString(fieldStaticFields, "A map of key/value pairs to add to each structured log.").Map().HasDefault(map[string]any{
			"@service": "bento",
		}),
		docs.FieldObject(fieldFile, "Experimental: Specify fields for optionally writing logs to a file.").WithChildren(
			docs.FieldString(fieldFilePath, "The file path to write logs to, if the file does not exist it will be created. Leave this field empty or unset to disable file based logging.").HasDefault(""),
			docs.FieldBool(fieldFileRotate, "Whether to rotate log files automatically.").HasDefault(false),
			docs.FieldInt(fieldFileRotateMaxAge, "The maximum number of days to retain old log files based on the timestamp encoded in their filename, after which they are deleted. Setting to zero disables this mechanism.").HasDefault(0),
		),
	}
}

//go:embed docs.md
var loggerDocs string

type loggerContext struct {
	Fields []docs.FieldSpecCtx
}

// DocsMarkdown returns a markdown document for the logger documentation.
func DocsMarkdown() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(loggerDocs)

	err := template.Must(template.New("logger").Parse(docs.FieldsTemplate(false)+`{{template "field_docs" . -}}`)).Execute(&buf, loggerContext{
		Fields: docs.FieldObject("", "").WithChildren(Spec()...).FlattenChildrenForDocs(),
	})

	return buf.Bytes(), err
}
