package pure

import (
	"fmt"
	"math/big"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

// twosComplementToBigInt decodes decimal logical-type bytes into a signed unscaled integer.
//
// The value is stored as two's complement big-endian bytes. Scale and precision come from
// the schema (Connect/Debezium/Iceberg), not from the byte payload.
//
//   https://docs.confluent.io/platform/current/connect/conversions.html#decimal-type
//   https://debezium.io/documentation/faq/#how_to_retrieve_decimal_field_from_binary_representation
//   https://iceberg.apache.org/spec/#decimal-type
func twosComplementToBigInt(val []byte) *big.Int {
	if len(val) == 0 {
		return big.NewInt(0)
	}

	if val[0]&0x80 != 0 {
		// MSB of the first byte is the sign bit. Negative values need two's complement
		// decode (~bytes + 1, then negate). Example: 0xFF is -1.
		invertedVal := make([]byte, len(val))
		for i, v := range val {
			invertedVal[i] = ^v
		}
		bigVal := new(big.Int).SetBytes(invertedVal)
		bigVal.Add(bigVal, big.NewInt(1))
		bigVal.Neg(bigVal)
		return bigVal
	}

	return new(big.Int).SetBytes(val)
}

// formatScaledDecimal turns an unscaled integer plus schema scale into a decimal string.
//
// Scale is the number of digits after the decimal point. unscaled 12345 at scale 2 is 123.45.
//
// https://debezium.io/documentation/faq/#how_to_retrieve_decimal_field_from_binary_representation
func formatScaledDecimal(unscaled *big.Int, scale int) string {
	if scale == 0 {
		return unscaled.String()
	}

	scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	rat := new(big.Rat).SetFrac(unscaled, scaleFactor)
	return rat.FloatString(scale)
}

func init() {
	parseBigDecimalSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryParsing).
		Description(`Parses a [Kafka Connect](https://docs.confluent.io/platform/current/connect/conversions.html#decimal-type) / [Debezium](https://debezium.io/documentation/faq/#how_to_retrieve_decimal_field_from_binary_representation) decimal encoded as a two's complement big-endian unscaled integer and returns its decimal string representation.`).
		Param(bloblang.NewInt64Param("scale").
			Description("Number of digits after the decimal point.")).
		Example("",
			`root.amount = this.amount.decode("base64").parse_big_decimal(scale: 2)`,
			[2]string{
				`{"amount":"MDk="}`,
				`{"amount":"123.45"}`,
			}).
		Example("",
			`root.amount = this.amount.decode("base64").parse_big_decimal(scale: 2)`,
			[2]string{
				`{"amount":"/w=="}`,
				`{"amount":"-0.01"}`,
			})

	if err := bloblang.RegisterMethodV2(
		"parse_big_decimal", parseBigDecimalSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			scale, err := args.GetInt64("scale")
			if err != nil {
				return nil, err
			}
			if scale < 0 {
				return nil, fmt.Errorf("scale must be >= 0, got %d", scale)
			}

			return bloblang.BytesMethod(func(input []byte) (any, error) {
				return formatScaledDecimal(twosComplementToBigInt(input), int(scale)), nil
			}), nil
		},
	); err != nil {
		panic(err)
	}
}
