package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// Commit pointers are a backup of the catalog. A catalog entry is only a
// pointer to the table's current metadata file, which lives next to the data;
// if the entry is lost (a dropped Glue table, a catalog outage that ends in a
// rebuild), the table is recovered by registering that file again. The
// highest-numbered metadata file is not necessarily the current one, because
// a commit that loses a conflict leaves its metadata file behind. So after
// every successful commit the output records which file is current:
//
//	{table location}/_bento/commits/v000000000042.json
//
// One file per table sequence number, written only after the commit succeeded,
// so concurrent replicas never overwrite each other's pointers. The highest
// sequence number is the latest committed version.
const (
	commitsDir = "_bento/commits"
	// commitPointersKept matches the number of metadata files the
	// table keeps (write.metadata.previous-versions-max): older pointers would
	// name metadata files that have been deleted.
	commitPointersKept = 100
)

type commitPointer struct {
	TableUUID        string    `json:"table_uuid"`
	SequenceNumber   int64     `json:"sequence_number"`
	SnapshotID       *int64    `json:"snapshot_id,omitempty"`
	MetadataLocation string    `json:"metadata_location"`
	WrittenAt        time.Time `json:"written_at"`
}

func commitPointerPath(tableLocation string, seq int64) string {
	return fmt.Sprintf("%s/%s/v%012d.json", strings.TrimSuffix(tableLocation, "/"), commitsDir, seq)
}

// parseCommitPointerName returns the sequence number of a pointer file name.
func parseCommitPointerName(name string) (int64, bool) {
	if !strings.HasPrefix(name, "v") || !strings.HasSuffix(name, ".json") {
		return 0, false
	}
	seq, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "v"), ".json"), 10, 64)
	return seq, err == nil && seq >= 0
}

// writeCommitPointer records tbl's current metadata file as the latest
// committed version, and removes the pointer that fell out of the kept range.
func writeCommitPointer(ctx context.Context, tbl *table.Table) error {
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	wfs, ok := fsys.(icebergio.WriteFileIO)
	if !ok {
		return fmt.Errorf("file IO %T cannot write files", fsys)
	}

	meta := tbl.Metadata()
	p := commitPointer{
		TableUUID:        meta.TableUUID().String(),
		SequenceNumber:   meta.LastSequenceNumber(),
		MetadataLocation: tbl.MetadataLocation(),
		WrittenAt:        time.Now().UTC(),
	}
	if snap := meta.CurrentSnapshot(); snap != nil {
		p.SnapshotID = &snap.SnapshotID
	}
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if err := wfs.WriteFile(commitPointerPath(tbl.Location(), p.SequenceNumber), b); err != nil {
		return err
	}
	if old := p.SequenceNumber - commitPointersKept; old >= 0 {
		// Best effort; a pointer left behind is harmless.
		_ = fsys.Remove(commitPointerPath(tbl.Location(), old))
	}
	return nil
}

// errTableNotRecoverable means the table's files exist at its location
// but the catalog has no entry for it and the current version is unknown.
// Creating a table there would hide the existing data, so the output refuses.
var errTableNotRecoverable = errors.New("table files exist but the catalog has no entry for the table")

// findCommittedMetadata returns the metadata file to register for a table
// that exists at location but is missing from the catalog, or "" if nothing
// was ever committed there and a new table can safely be created.
func findCommittedMetadata(fsys icebergio.IO, location string) (string, error) {
	location = strings.TrimSuffix(location, "/")
	latest, err := latestCommitPointer(fsys, location)
	if err != nil {
		return "", err
	}
	if latest != nil {
		return latest.MetadataLocation, nil
	}

	// No pointers. A metadata file with a snapshot means rows were committed
	// (or a commit was attempted) by something that left no pointer; which
	// file is current can't be told safely.
	var dataFound bool
	err = walkFiles(fsys, location+"/metadata", func(path, name string) error {
		if !strings.HasSuffix(name, ".metadata.json") {
			return nil
		}
		if hasData, err := metadataHasSnapshot(fsys, path); err != nil || hasData {
			dataFound = true
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if dataFound {
		return "", fmt.Errorf("%w: no %s pointers under %s; register the current metadata file under %s/metadata with the catalog", errTableNotRecoverable, commitsDir, location, location)
	}
	return "", nil
}

func latestCommitPointer(fsys icebergio.IO, location string) (*commitPointer, error) {
	var (
		latestSeq  int64 = -1
		latestPath string
	)
	err := walkFiles(fsys, location+"/"+commitsDir, func(path, name string) error {
		if seq, ok := parseCommitPointerName(name); ok && seq > latestSeq {
			latestSeq, latestPath = seq, path
		}
		return nil
	})
	if err != nil || latestPath == "" {
		return nil, err
	}

	b, err := readFile(fsys, latestPath)
	if err != nil {
		return nil, fmt.Errorf("reading commit pointer: %w", err)
	}
	var p commitPointer
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("parsing commit pointer %s: %w", latestPath, err)
	}
	if p.MetadataLocation == "" {
		return nil, fmt.Errorf("commit pointer %s has no metadata_location", latestPath)
	}
	return &p, nil
}

// walkFiles calls fn with the full path and base name of every file in dir.
// A missing dir has no files.
func walkFiles(fsys icebergio.IO, dir string, fn func(path, name string) error) error {
	lfs, ok := fsys.(icebergio.ListableIO)
	if !ok {
		return fmt.Errorf("file IO %T cannot list files", fsys)
	}
	rootSeen := false
	return lfs.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			// The first directory is dir itself; don't descend into others.
			if rootSeen {
				return fs.SkipDir
			}
			rootSeen = true
			return nil
		}
		// Rebuild the path from dir: WalkDir paths are OS paths for local
		// files but URIs for object stores.
		return fn(dir+"/"+d.Name(), d.Name())
	})
}

func metadataHasSnapshot(fsys icebergio.IO, path string) (bool, error) {
	b, err := readFile(fsys, path)
	if err != nil {
		return false, err
	}
	meta, err := table.ParseMetadataBytes(b)
	if err != nil {
		return false, err
	}
	return meta.CurrentSnapshot() != nil || len(meta.Snapshots()) > 0, nil
}

func readFile(fsys icebergio.IO, path string) ([]byte, error) {
	rfs, ok := fsys.(icebergio.ReadFileIO)
	if !ok {
		return nil, fmt.Errorf("file IO %T cannot read files", fsys)
	}
	return rfs.ReadFile(path)
}
