package pure

import (
	"errors"
	"math"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

var (
	errVectorInvalidType   = errors.New("vector must be an array of numeric types")
	errVectorNull          = errors.New("vectors must not be null (all zeros)")
	errVectorUnequalLength = errors.New("vectors must be equal length")
	errVectorEmpty         = errors.New("vectors cannot be empty")
)

func init() {
	cosineSimilaritySpec := bloblang.NewPluginSpec().
		Category(query.FunctionCategoryGeneral).
		Description("Calculates the cosine similarity between two vectors a and b. Vectors must be the same length and neither vector can be null (all zeros).").
		Param(bloblang.NewAnyParam("a").Description("Vector of numbers.")).
		Param(bloblang.NewAnyParam("b").Description("Vector of numbers.")).
		Example(
			"Calculate similarity between vectors",
			`root.similarity = cosine_similarity([1, 2, 3], [2, 4, 6])`,
			[2]string{`{}`, `{"similarity":1}`},
		).
		Example(
			"Orthogonal vectors have zero similarity",
			`root.similarity = cosine_similarity([1, 0], [0, 1])`,
			[2]string{`{}`, `{"similarity":0}`},
		)

	if err := bloblang.RegisterFunctionV2(
		"cosine_similarity", cosineSimilaritySpec,
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {

			aVecArg, err := args.Get("a")
			if err != nil {
				return nil, err
			}

			bVecArg, err := args.Get("b")
			if err != nil {
				return nil, err
			}

			return func() (any, error) {
				aVec, err := toFloat64(aVecArg)
				if err != nil {
					return nil, err
				}

				bVec, err := toFloat64(bVecArg)
				if err != nil {
					return nil, err
				}

				return cosine(aVec, bVec)
			}, nil
		},
	); err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func cosine(a, b []float64) (cosine float64, err error) {
	if len(a) != len(b) {
		return 0, errVectorUnequalLength
	}

	if len(a) == 0 || len(b) == 0 {
		return 0, errVectorEmpty
	}

	var dotProduct, ssA, ssB float64
	for i := 0; i < len(a); i++ {
		fa, fb := a[i], b[i]
		dotProduct += fa * fb
		ssA += fa * fa
		ssB += fb * fb
	}

	if ssA == 0 || ssB == 0 {
		return 0.0, errVectorNull
	}
	return dotProduct / (math.Sqrt(ssA) * math.Sqrt(ssB)), nil
}

func toFloat64(v any) ([]float64, error) {
	switch vals := v.(type) {
	case []float64:
		return vals, nil
	case []float32:
		result := make([]float64, len(vals))
		for i, val := range vals {
			result[i] = float64(val)
		}
		return result, nil
	case []int:
		result := make([]float64, len(vals))
		for i, val := range vals {
			result[i] = float64(val)
		}
		return result, nil
	case []int32:
		result := make([]float64, len(vals))
		for i, val := range vals {
			result[i] = float64(val)
		}
		return result, nil
	case []int64:
		result := make([]float64, len(vals))
		for i, val := range vals {
			result[i] = float64(val)
		}
		return result, nil
	case []any:
		result := make([]float64, len(vals))
		for i, val := range vals {
			switch num := val.(type) {
			case float64:
				result[i] = num
			case float32:
				result[i] = float64(num)
			case int:
				result[i] = float64(num)
			case int32:
				result[i] = float64(num)
			case int64:
				result[i] = float64(num)
			default:
				return nil, errVectorInvalidType
			}
		}
		return result, nil
	default:
		return nil, errVectorInvalidType
	}
}
