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

		// Positive values.
		{name: "1", input: []byte{0x01}, expected: big.NewInt(1)},
		{name: "127", input: []byte{0x7F}, expected: big.NewInt(127)},
		{name: "128", input: []byte{0x00, 0x80}, expected: big.NewInt(128)},
		{name: "255", input: []byte{0x00, 0xFF}, expected: big.NewInt(255)},
		{name: "256", input: []byte{0x01, 0x00}, expected: big.NewInt(256)},
		{name: "32767", input: []byte{0x7F, 0xFF}, expected: big.NewInt(32767)},
		{name: "32768", input: []byte{0x00, 0x80, 0x00}, expected: big.NewInt(32768)},
		{name: "12345", input: []byte{0x30, 0x39}, expected: big.NewInt(12345)},

		// Negative values.
		{name: "-1", input: []byte{0xFF}, expected: big.NewInt(-1)},
		{name: "-127", input: []byte{0x81}, expected: big.NewInt(-127)},
		{name: "-128", input: []byte{0x80}, expected: big.NewInt(-128)},
		{name: "-129", input: []byte{0xFF, 0x7F}, expected: big.NewInt(-129)},
		{name: "-256", input: []byte{0xFF, 0x00}, expected: big.NewInt(-256)},
		{name: "-1050", input: []byte{0xFB, 0xE6}, expected: big.NewInt(-1050)},
		{name: "-32768", input: []byte{0x80, 0x00}, expected: big.NewInt(-32768)},
		{name: "-32769", input: []byte{0xFF, 0x7F, 0xFF}, expected: big.NewInt(-32769)},

		// Negative powers of 2.
		{name: "-2", input: []byte{0xFE}, expected: big.NewInt(-2)},
		{name: "-64", input: []byte{0xC0}, expected: big.NewInt(-64)},
		{name: "-512", input: []byte{0xFE, 0x00}, expected: big.NewInt(-512)},
		{name: "-1024", input: []byte{0xFC, 0x00}, expected: big.NewInt(-1024)},

		// Edge cases.
		{name: "nil input", input: nil, expected: big.NewInt(0)},
		{name: "empty input", input: []byte{}, expected: big.NewInt(0)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := twosComplementToBigInt(tc.input)
			require.Equal(t, 0, tc.expected.Cmp(actual),
				"expected %s but got %s", tc.expected, actual)
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
