//go:build ignore

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/andybalholm/brotli"
)

const (
	wasmBinaryUrl = "https://github.com/vmware-labs/webassembly-language-runtimes/releases/latest/download/python-3.12.0.wasm"

	// Retrieved from https://github.com/vmware-labs/webassembly-language-runtimes/releases/download/python%2F3.12.0%2B20231211-040d5a6/python-3.12.0.wasm.sha256sum
	wasmBinarySha256 = "e5dc5a398b07b54ea8fdb503bf68fb583d533f10ec3f930963e02b9505f7a763"
)

func main() {
	downloadUrlPtr := flag.String("url", wasmBinaryUrl, "Destination of file.")
	outputPathPtr := flag.String("out", ".", "Output location of downloaded file.")
	hashPtr := flag.String("hash", wasmBinarySha256, "SHA256 of downloaded file.")

	flag.Parse()

	switch {
	case hashPtr == nil:
		panic("'hash' flag cannot be empty")
	case outputPathPtr == nil:
		panic("'out' flag cannot be empty.")
	case outputPathPtr == nil:
		panic("'out' flag cannot be empty.")
	}

	err := download(*downloadUrlPtr, *outputPathPtr, *hashPtr)
	if err != nil {
		log.Fatalf("failed to download and verify file: %s", err.Error())
	}

}

func download(from, to, expectedHash string) error {
	fileName := filepath.Base(from)
	outPath := filepath.Join(to, fileName+".br")

	if isValidLocalFile(outPath, expectedHash) {
		fmt.Printf("File %s already exists and matches hash. Skipping download.\n", outPath)
		return nil
	}

	resp, err := http.Get(from)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	bw := brotli.NewWriterLevel(outFile, brotli.BestCompression)
	defer bw.Close()

	h := sha256.New()
	writer := io.MultiWriter(h, bw)

	if _, err := io.Copy(writer, resp.Body); err != nil {
		return err
	}

	actualHash := hex.EncodeToString(h.Sum(nil))
	if actualHash != expectedHash {
		os.Remove(outPath)
		return fmt.Errorf("hash mismatch: expected '%s', got '%s'", expectedHash, actualHash)
	}

	return nil
}

func isValidLocalFile(path, expectedHash string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	br := brotli.NewReader(f)
	h := sha256.New()

	if _, err := io.Copy(h, br); err != nil {
		return false
	}

	return hex.EncodeToString(h.Sum(nil)) == expectedHash
}
