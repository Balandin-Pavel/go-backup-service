package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateArchive(t *testing.T) {
	sourceRoot := t.TempDir()
	outputRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(sourceRoot, "photos"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "photos", "note.txt"), []byte("hello backup"), 0600); err != nil {
		t.Fatal(err)
	}
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	name, size, err := CreateArchive(context.Background(), sourceRoot, outputRoot, "photos", id)
	if err != nil {
		t.Fatal(err)
	}
	if name != id+".tar.gz" || size <= 0 {
		t.Fatalf("unexpected archive metadata: %q, %d", name, size)
	}
	file, err := os.Open(filepath.Join(outputRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	found := false
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "photos/note.txt" {
			body, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != "hello backup" {
				t.Fatalf("wrong content: %q", body)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("file is missing from archive")
	}
}

func TestRejectTraversalAndSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if _, err := ValidateSource(root, "../elsewhere"); err == nil {
		t.Fatal("accepted traversal")
	}
	if _, err := ValidateSource(root, outside); err == nil {
		t.Fatal("accepted absolute path")
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateSource(root, "link"); err == nil {
		t.Fatal("accepted symlink source")
	}
	if err := os.Mkdir(filepath.Join(root, "folder"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "folder", "nested-link")); err != nil {
		t.Fatal(err)
	}
	_, _, err := CreateArchive(context.Background(), root, t.TempDir(), "folder", "test")
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("expected nested symlink error, got %v", err)
	}
}

func TestCancelledArchiveLeavesNoFile(t *testing.T) {
	root := t.TempDir()
	out := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := CreateArchive(ctx, root, out, ".", "cancelled")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("left temporary files: %v", entries)
	}
}
