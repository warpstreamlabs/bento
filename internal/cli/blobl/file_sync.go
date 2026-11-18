package blobl

import (
	"errors"
	"io/fs"
	"log"
	"sync"
	"time"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

// fileSync manages synchronized file reading/writing for playground state
type fileSync struct {
	mut sync.Mutex

	dirty         bool
	mappingString string
	inputString   string

	writeBack   bool
	mappingFile string
	inputFile   string
}

// newFileSync creates a new fileSync instance and optionally starts background writer
func newFileSync(inputFile, mappingFile string, writeBack bool) *fileSync {
	f := &fileSync{
		inputString:   defaultPlaygroundInput,
		mappingString: defaultPlaygroundMapping,
		writeBack:     writeBack,
		inputFile:     inputFile,
		mappingFile:   mappingFile,
	}

	if inputFile != "" {
		inputBytes, err := ifs.ReadFile(ifs.OS(), inputFile)
		if err != nil {
			if !writeBack || !errors.Is(err, fs.ErrNotExist) {
				log.Fatal(err)
			}
		} else {
			f.inputString = string(inputBytes)
		}
	}

	if mappingFile != "" {
		mappingBytes, err := ifs.ReadFile(ifs.OS(), mappingFile)
		if err != nil {
			if !writeBack || !errors.Is(err, fs.ErrNotExist) {
				log.Fatal(err)
			}
		} else {
			f.mappingString = string(mappingBytes)
		}
	}

	if writeBack {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				f.write()
			}
		}()
	}

	return f
}

func (f *fileSync) update(input, mapping string) {
	f.mut.Lock()
	defer f.mut.Unlock()

	if mapping != f.mappingString || input != f.inputString {
		f.dirty = true
	}
	f.mappingString = mapping
	f.inputString = input
}

func (f *fileSync) write() {
	f.mut.Lock()
	defer f.mut.Unlock()

	if !f.writeBack || !f.dirty {
		return
	}

	if f.inputFile != "" {
		if err := ifs.WriteFile(ifs.OS(), f.inputFile, []byte(f.inputString), 0o644); err != nil {
			log.Printf("Failed to write input file: %v\n", err)
		}
	}

	if f.mappingFile != "" {
		if err := ifs.WriteFile(ifs.OS(), f.mappingFile, []byte(f.mappingString), 0o644); err != nil {
			log.Printf("Failed to write mapping file: %v\n", err)
		}
	}

	f.dirty = false
}

func (f *fileSync) input() string {
	f.mut.Lock()
	defer f.mut.Unlock()
	return f.inputString
}

func (f *fileSync) mapping() string {
	f.mut.Lock()
	defer f.mut.Unlock()
	return f.mappingString
}
