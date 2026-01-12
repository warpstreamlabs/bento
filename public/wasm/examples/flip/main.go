package main

import (
	"context"
	"strings"

	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/batch_processor"
)

func init() {
	plugin.RegisterBatchProcessor(newFlipProcessor)
}

func newFlipProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return &flipProc{}, nil
}

var upsideDownChars = map[string]string{
	"z": "z", "y": "ʎ", "x": "x", "w": "ʍ", "v": "ʌ", "u": "n", "t": "ʇ",
	"s": "s", "r": "ɹ", "q": "b", "p": "d", "o": "o", "n": "u", "m": "ɯ",
	"l": "l", "k": "ʞ", "j": "ɾ", "i": "ᴉ", "h": "ɥ", "g": "ƃ", "f": "ɟ",
	"e": "ǝ", "d": "p", "c": "ɔ", "b": "q", "a": "ɐ", " ": " ", "-": "-",
	"+": "+", "Z": "Z", "Y": "⅄", "X": "X", "W": "M", "V": "Λ", "U": "∩",
	"T": "┴", "S": "S", "R": "ɹ", "Q": "Q", "P": "Ԁ", "O": "O", "N": "N",
	"M": "W", "L": "˥", "K": "ʞ", "J": "ſ", "I": "I", "H": "H", "G": "פ",
	"F": "Ⅎ", "E": "Ǝ", "D": "p", "C": "Ɔ", "B": "q", "A": "∀", "9": "6",
	"8": "8", "7": "ㄥ", "6": "9", "5": "ϛ", "4": "ㄣ", "3": "Ɛ",
	"2": "ᄅ", "1": "Ɩ", "0": "0",
}

type flipProc struct{}

func flip(s string) string {
	var result strings.Builder

	for _, char := range s {
		charStr := string(char)
		if flipped, ok := upsideDownChars[charStr]; ok {
			result.WriteString(flipped)
		} else {
			result.WriteRune(char)
		}
	}

	return result.String()
}

func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

func (p *flipProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	outBatch := make(service.MessageBatch, len(batch))
	for i, msg := range batch {
		data, _ := msg.AsBytes()
		inputStr := string(data)
		flipped := flip(inputStr)
		outBatch[i] = service.NewMessage([]byte(flipped))
	}
	return []service.MessageBatch{outBatch}, nil
}

func (p *flipProc) Close(ctx context.Context) error {
	return nil
}

func main() {}
