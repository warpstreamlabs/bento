package pure

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"unicode"
	"unicode/utf8"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

// var compressAlgorithms = map[string]

func init() {
	if err := bloblang.RegisterMethodV2("parse_form_url_encoded",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryParsing).
			Description(`Attempts to parse a url-encoded query string (from an x-www-form-urlencoded request body) and returns a structured result.`).
			Example("", `root.values = this.body.parse_form_url_encoded()`,
				[2]string{
					`{"body":"noise=meow&animal=cat&fur=orange&fur=fluffy"}`,
					`{"values":{"animal":"cat","fur":["orange","fluffy"],"noise":"meow"}}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.StringMethod(func(data string) (any, error) {
				values, err := url.ParseQuery(data)
				if err != nil {
					return nil, fmt.Errorf("failed to parse value as url-encoded data: %w", err)
				}
				return urlValuesToMap(values), nil
			}), nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("is_unicode",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryParsing).
			Description("Returns whether a byte array consists entirely of valid Unicode characters. Checks both UTF-8 encoding validity and rejects Unicode noncharacters (U+FFFE, U+FFFF, etc.) and surrogates.").
			Example("Invalid UTF-8 bytes return false.", `root = this.body.decode("base64").is_unicode()`,
				[2]string{
					fmt.Sprintf(`{"body":"%s"}`, base64.StdEncoding.EncodeToString([]byte{0xff, 0xfe, 0x00})),
					`false`,
				},
			).
			Example("Valid Unicode string returns true.", `root = this.bytes().is_unicode()`,
				[2]string{
					`"I'm a simple string."`,
					`true`,
				},
			).
			Example("Unicode non-characters return false.", `root = this.body.decode("base64").is_unicode()`,
				[2]string{
					fmt.Sprintf(`{"body":"%s"}`, base64.StdEncoding.EncodeToString([]byte{0xef, 0xbf, 0xbe})),
					`false`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.BytesMethod(func(data []byte) (any, error) {
				if !utf8.Valid(data) {
					return false, nil
				}

				// Check for Unicode noncharacters
				for _, r := range string(data) {
					if unicode.Is(unicode.Noncharacter_Code_Point, r) {
						return false, nil
					}
				}

				return true, nil
			}), nil
		}); err != nil {
		panic(err)
	}

}

func urlValuesToMap(values url.Values) map[string]any {
	root := make(map[string]any, len(values))

	for k, v := range values {
		if len(v) == 1 {
			root[k] = v[0]
		} else {
			elements := make([]any, 0, len(v))
			for _, e := range v {
				elements = append(elements, e)
			}
			root[k] = elements
		}
	}

	return root
}
