package parser

import (
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// queryParserBuiltHook, when non-nil, is invoked every time queryParser
// actually builds a new parser tree (i.e. on a cache miss). It exists purely
// to make cache behaviour observable from tests, since comparing Func[T]
// values structurally isn't possible in Go (reflect.ValueOf(fn).Pointer()
// can return the same code address for structurally identical closures even
// when they capture different environments). It must never be set outside
// of tests.
var queryParserBuiltHook func()

func queryParser(pCtx Context) Func[query.Function] {
	key := pCtx.cacheKey()
	if cached, ok := cachedParser[query.Function](pCtx, key); ok {
		return cached
	}

	if queryParserBuiltHook != nil {
		queryParserBuiltHook()
	}

	rootParser := parseWithTails(Expect(
		OneOf(
			matchExpressionParser(pCtx),
			ifExpressionParser(pCtx),
			lambdaExpressionParser(pCtx),
			bracketsExpressionParser(pCtx),
			literalValueParser(pCtx),
			functionParser(pCtx),
			metadataReferenceParser,
			variableReferenceParser,
			fieldReferenceRootParser(pCtx),
		),
		"query",
	), pCtx)

	built := func(input []rune) Result[query.Function] {
		res := SpacesAndTabs(input)
		return arithmeticParser(rootParser)(res.Remaining)
	}

	storeParser(pCtx, key, built)
	return built
}

func tryParseQuery(expr string) (query.Function, *Error) {
	res := queryParser(Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
	})([]rune(expr))
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Payload, nil
}
