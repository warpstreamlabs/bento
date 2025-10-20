package pure

import (
	"context"
	"fmt"
	"io"

	"github.com/clbanning/mxj/v2"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	pFieldOperator = "operator"
	pFieldCast     = "cast"
)

func xmlDocumentScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Consumes a stream of one or more XML documents and performs a mutation on the data.").
		Fields(
			service.NewStringEnumField(pFieldOperator, "to_json").
				Description("An XML [operation](#operators) to apply to messages.").
				Default(""),
			service.NewBoolField(pFieldCast).
				Description("Whether to try to cast values that are numbers and booleans to the right type. Default: all values are strings.").
				Default(false),
		)

}

func init() {
	err := service.RegisterBatchScannerCreator("xml_documents", xmlDocumentScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return xmlDocumentScannerCreatorFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type xmlDocumentScannerCreator struct {
	cast bool
}

func xmlDocumentScannerCreatorFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*xmlDocumentScannerCreator, error) {
	operator, err := pConf.FieldString(pFieldOperator)
	if err != nil {
		return nil, err
	}
	if operator != "to_json" {
		return nil, fmt.Errorf("operator not recognised: %v", operator)
	}

	cast, err := pConf.FieldBool(pFieldCast)
	if err != nil {
		return nil, err
	}

	j := &xmlDocumentScannerCreator{
		cast: cast,
	}
	return j, nil
}

func (xs *xmlDocumentScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	return service.AutoAggregateBatchScannerAcks(&xmlDocumentScanner{
		r: rdr,
	}, aFn), nil
}

func (xs *xmlDocumentScannerCreator) Close(context.Context) error {
	return nil
}

type xmlDocumentScanner struct {
	r io.ReadCloser
}

func (xs *xmlDocumentScanner) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	if xs.r == nil {
		return nil, io.EOF
	}

	m, err := mxj.NewMapXmlReader(xs.r)
	if err != nil {
		_ = xs.r.Close()
		xs.r = nil
		return nil, err
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]any(m))

	return service.MessageBatch{msg}, nil
}

func (xs *xmlDocumentScanner) Close(ctx context.Context) error {
	if xs.r == nil {
		return nil
	}
	return xs.r.Close()
}
