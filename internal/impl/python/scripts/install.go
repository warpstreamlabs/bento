//go:build ignore

package main

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
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

	resp, err := http.Get(from)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	content := bytes.NewBuffer(nil)
	h := sha256.New()

	tee := io.TeeReader(resp.Body, h)
	if _, err := io.Copy(content, tee); err != nil {
		return err
	}

	actualHash := hex.EncodeToString(h.Sum(nil))
	if actualHash != expectedHash {
		return fmt.Errorf("hash mismatch: expected '%s', got '%s'", expectedHash, actualHash)
	}

	return os.WriteFile(filepath.Join(to, fileName), content.Bytes(), 0644)
}
