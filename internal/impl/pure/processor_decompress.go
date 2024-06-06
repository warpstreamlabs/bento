package pure

import (
	"context"
	"fmt"

	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/component/interop"
	"github.com/warpstreamlabs/bento/v1/internal/component/processor"
	"github.com/warpstreamlabs/bento/v1/internal/log"
	"github.com/warpstreamlabs/bento/v1/internal/message"
	"github.com/warpstreamlabs/bento/v1/public/service"
)

const (
	decompressPFieldAlgorithm = "algorithm"
)

func init() {
	compAlgs := DecompressionAlgsList()
	err := service.RegisterBatchProcessor(
		"decompress", service.NewConfigSpec().
			Categories("Parsing").
			Stable().
			Summary(fmt.Sprintf("Decompresses messages according to the selected algorithm. Supported decompression algorithms are: %v", compAlgs)).
			Fields(
				service.NewStringEnumField(decompressPFieldAlgorithm, compAlgs...).
					Description("The decompression algorithm to use.").
					LintRule(``),
			),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			algStr, err := conf.FieldString(compressPFieldAlgorithm)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newDecompress(algStr, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedProcessor("decompress", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type decompressProc struct {
	decomp DecompressFunc
	log    log.Modular
}

func newDecompress(algStr string, mgr bundle.NewManagement) (*decompressProc, error) {
	dcor, err := strToDecompressFunc(algStr)
	if err != nil {
		return nil, err
	}
	return &decompressProc{
		decomp: dcor,
		log:    mgr.Logger(),
	}, nil
}

func (d *decompressProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	newBytes, err := d.decomp(msg.AsBytes())
	if err != nil {
		d.log.Error("Failed to decompress message part: %v\n", err)
		return nil, err
	}

	msg.SetBytes(newBytes)
	return []*message.Part{msg}, nil
}

func (d *decompressProc) Close(context.Context) error {
	return nil
}
