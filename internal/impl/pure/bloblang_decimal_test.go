package pure

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTwosComplementToBigInt(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected *big.Int
	}{
		{name: "zero", input: []byte{0x00}, expected: big.NewInt(0)},
		{name: "1", input: []byte{0x01}, expected: big.NewInt(1)},
		{name: "12345", input: []byte{0x30, 0x39}, expected: big.NewInt(12345)},
		{name: "-1", input: []byte{0xFF}, expected: big.NewInt(-1)},
		{name: "-1050", input: []byte{0xFB, 0xE6}, expected: big.NewInt(-1050)},
		{name: "nil input", input: nil, expected: big.NewInt(0)},
		{name: "empty input", input: []byte{}, expected: big.NewInt(0)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := twosComplementToBigInt(tc.input)
			require.Equal(t, 0, tc.expected.Cmp(actual))
		})
	}
}

func TestFormatScaledDecimal(t *testing.T) {
	tests := []struct {
		name     string
		unscaled *big.Int
		scale    int
		expected string
	}{
		{name: "zero scale 0", unscaled: big.NewInt(0), scale: 0, expected: "0"},
		{name: "positive scale 2", unscaled: big.NewInt(1), scale: 2, expected: "0.01"},
		{name: "123.45", unscaled: big.NewInt(12345), scale: 2, expected: "123.45"},
		{name: "negative fractional", unscaled: big.NewInt(-1), scale: 2, expected: "-0.01"},
		{name: "negative whole", unscaled: big.NewInt(-1050), scale: 2, expected: "-10.50"},
		{name: "zero scale 2", unscaled: big.NewInt(0), scale: 2, expected: "0.00"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, formatScaledDecimal(tc.unscaled, tc.scale))
		})
	}
}
