package oci

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/oci"
)

// Save writes plugins to w as a tar archive of an OCI image layout, the same
// format docker save and oras use, tagged with their full references.
func (s *Store) Save(ctx context.Context, references []string, w io.Writer) error {
	if len(references) == 0 {
		return errors.New("no plugins to save")
	}

	tmp, err := os.MkdirTemp("", "bento-plugin-save-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	layout, err := oci.NewWithContext(ctx, tmp)
	if err != nil {
		return err
	}
	err = s.locked(ctx, false, func() error {
		for _, r := range references {
			ref, err := ParseReference(r)
			if err != nil {
				return err
			}
			desc, err := s.resolve(ctx, ref)
			if err != nil {
				return err
			}
			if _, err := readPlugin(ctx, s.oci, desc, false); err != nil {
				return fmt.Errorf("%v: %w", ref, err)
			}
			if _, err := oras.Copy(ctx, s.oci, desc.Digest.String(), layout, ref.String(), oras.DefaultCopyOptions); err != nil {
				return fmt.Errorf("failed to save %v: %w", ref, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return tarDir(tmp, w)
}

// Load reads plugins from a tar archive of an OCI image layout into the
// store, keeping their tags. It returns the tags loaded. Every plugin is
// checked before any are stored.
func (s *Store) Load(ctx context.Context, path string) ([]string, error) {
	archive, err := oci.NewFromTar(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %v: %w", path, err)
	}

	var tags []string
	if err := archive.Tags(ctx, "", func(t []string) error {
		tags = append(tags, t...)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(tags) == 0 {
		return nil, fmt.Errorf("%v contains no tagged plugins", path)
	}

	descs := make([]ocispec.Descriptor, len(tags))
	for i, tag := range tags {
		if _, err := ParseReference(tag); err != nil {
			return nil, err
		}
		if descs[i], err = archive.Resolve(ctx, tag); err != nil {
			return nil, err
		}
		if _, err := readPlugin(ctx, archive, descs[i], true); err != nil {
			return nil, fmt.Errorf("%v: %w", tag, err)
		}
	}

	err = s.locked(ctx, true, func() error {
		for i, tag := range tags {
			if _, err := oras.Copy(ctx, archive, descs[i].Digest.String(), s.oci, tag, oras.DefaultCopyOptions); err != nil {
				return fmt.Errorf("failed to load %v: %w", tag, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tags, nil
}

func tarDir(dir string, w io.Writer) error {
	tw := tar.NewWriter(w)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || rel == "." {
			return err
		}
		// The store's scratch space for incomplete writes, which isn't part of
		// the layout.
		if rel == "ingest" {
			return fs.SkipDir
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		// Fixed times and owners, so saving the same plugins twice gives the
		// same archive.
		hdr.ModTime, hdr.AccessTime, hdr.ChangeTime = time.Unix(0, 0), time.Time{}, time.Time{}
		hdr.Uid, hdr.Gid, hdr.Uname, hdr.Gname = 0, 0, "", ""
		if d.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return err
	}
	return tw.Close()
}
