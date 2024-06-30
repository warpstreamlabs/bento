package codec

import (
	"context"
	"io"

	"github.com/warpstreamlabs/bento/internal/codec"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fieldCodecFromString = "codec"
	crFieldCodec         = "scanner"
	crFieldMaxBuffer     = "max_buffer"
)

// DeprecatedCodecFields contain definitions for deprecated codec fields that
// allow backwards compatible migration towards the new scanner field.
//
// New plugins should instead use the new scanner fields.
func DeprecatedCodecFields(defaultScanner string) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewInternalField(codec.NewReaderDocs(fieldCodecFromString)).Deprecated().Optional(),
		service.NewIntField(crFieldMaxBuffer).Deprecated().Default(1000000),
		service.NewScannerField(crFieldCodec).
			Description("The [scanner](/docs/components/scanners/about) by which the stream of bytes consumed will be broken out into individual messages. Scanners are useful for processing large sources of data without holding the entirety of it within memory. For example, the `csv` scanner allows you to process individual CSV rows without loading the entire CSV file in memory at once.").
			Default(map[string]any{defaultScanner: map[string]any{}}).
			Version("1.0.0").
			Optional(),
	}
}

// DeprecatedFallbackCodec provides a common interface that abstracts either an
// old codec implementation or a new scanner.
type DeprecatedFallbackCodec interface {
	Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (DeprecatedFallbackStream, error)
	Close(context.Context) error
}

// DeprecatedFallbackStream provides a common interface that abstracts either an
// old codec implementation or a new scanner.
type DeprecatedFallbackStream interface {
	NextBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error)
	Close(context.Context) error
}

// DeprecatedCodecFromParsed attempts to create a deprecated fallback codec from
// a parsed config.
func DeprecatedCodecFromParsed(conf *service.ParsedConfig) (DeprecatedFallbackCodec, error) {
	if conf.Contains(fieldCodecFromString) {
		codecName, err := conf.FieldString(fieldCodecFromString)
		if err != nil {
			return nil, err
		}

		maxBuffer, _ := conf.FieldInt(crFieldMaxBuffer)
		if maxBuffer == 0 {
			maxBuffer = 1000000
		}

		oldCtor, err := codec.GetReader(codecName, codec.ReaderConfig{
			MaxScanTokenSize: maxBuffer,
		})
		if err != nil {
			return nil, err
		}
		return &codecRInternal{oldCtor}, nil
	}

	ownedCodec, err := conf.FieldScanner(crFieldCodec)
	if err != nil {
		return nil, err
	}
	return &codecRPublic{newCtor: ownedCodec}, nil
}

type codecRInternal struct {
	oldCtor codec.ReaderConstructor
}

func (r *codecRInternal) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (DeprecatedFallbackStream, error) {
	oldR, err := r.oldCtor(details.Name(), rdr, codec.ReaderAckFn(aFn))
	if err != nil {
		return nil, err
	}
	return &streamRInternal{oldR}, nil
}

func (r *codecRInternal) Close(ctx context.Context) error {
	return nil
}

type streamRInternal struct {
	old codec.Reader
}

func (r *streamRInternal) NextBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	ib, aFn, err := r.old.Next(ctx)
	if err != nil {
		return nil, nil, err
	}

	batch := make(service.MessageBatch, len(ib))
	for i := range ib {
		batch[i] = service.NewInternalMessage(ib[i])
	}
	return batch, service.AckFunc(aFn), nil
}

func (r *streamRInternal) Close(ctx context.Context) error {
	return r.old.Close(ctx)
}

type codecRPublic struct {
	newCtor *service.OwnedScannerCreator
}

func (r *codecRPublic) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (DeprecatedFallbackStream, error) {
	sDetails := service.NewScannerSourceDetails()
	sDetails.SetName(details.Name())
	return r.newCtor.Create(rdr, aFn, sDetails)
}

func (r *codecRPublic) Close(ctx context.Context) error {
	return r.newCtor.Close(ctx)
}
