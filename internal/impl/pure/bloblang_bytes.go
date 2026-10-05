package pure

import (
	"github.com/dustin/go-humanize"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("parse_bytes",
		bloblang.NewPluginSpec().
			Static().
			Category(query.MethodCategoryParsing).
			Description(`Attempts to parse a string as a humanised byte size (such as "10MB", "20MiB" or "1Kb") and returns an integer of the equivalent number of bytes, using the `+"[dustin/go-humanize](https://github.com/dustin/go-humanize)"+` library.`).
			Example("",
				`root.max_bytes = this.max_bytes_str.parse_bytes()`,
				[2]string{
					`{"max_bytes_str":"10MiB"}`,
					`{"max_bytes":10485760}`,
				},
			).
			Example("",
				`root.byte_size_limit = this.byte_size_limit_str.parse_bytes()`,
				[2]string{
					`{"byte_size_limit_str":"10MB"}`,
					`{"byte_size_limit":10000000}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.StringMethod(func(s string) (any, error) {
				return humanize.ParseBytes(s)
			}), nil
		}); err != nil {
		panic(err)
	}
}
