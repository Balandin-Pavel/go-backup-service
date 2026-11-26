package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func ValidateSource(root, source string) (string, error) {
	if source == "" || !filepath.IsLocal(source) {
		return "", fmt.Errorf("source must be a relative path inside the configured root")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	clean := filepath.Clean(source)
	path := resolvedRoot
	for _, part := range strings.Split(clean, string(filepath.Separator)) {
		if part == "." {
			continue
		}
		path = filepath.Join(path, part)
		info, err := os.Lstat(path)
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("source contains a symbolic link")
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("source must be a directory")
	}
	return path, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c contextReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func CreateArchive(ctx context.Context, sourceRoot, outputRoot, source, id string) (string, int64, error) {
	sourcePath, err := ValidateSource(sourceRoot, source)
	if err != nil {
		return "", 0, err
	}
	if err := os.MkdirAll(outputRoot, 0700); err != nil {
		return "", 0, err
	}
	temp, err := os.CreateTemp(outputRoot, id+"-*.tmp")
	if err != nil {
		return "", 0, err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()

	gz := gzip.NewWriter(temp)
	tw := tar.NewWriter(gz)
	archiveRoot := filepath.Base(sourcePath)
	if archiveRoot == "." || archiveRoot == string(filepath.Separator) {
		archiveRoot = "data"
	}
	err = filepath.WalkDir(sourcePath, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symbolic link is not allowed: %s", entry.Name())
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported file type: %s", entry.Name())
		}
		relative, err := filepath.Rel(sourcePath, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(filepath.Join(archiveRoot, relative))
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = name
		if info.IsDir() {
			header.Name += "/"
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, contextReader{ctx: ctx, r: file})
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		return err
	})
	if err != nil {
		return "", 0, err
	}
	if err := tw.Close(); err != nil {
		return "", 0, err
	}
	if err := gz.Close(); err != nil {
		return "", 0, err
	}
	if err := temp.Sync(); err != nil {
		return "", 0, err
	}
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	info, err := temp.Stat()
	if err != nil {
		return "", 0, err
	}
	name := id + ".tar.gz"
	if err := os.Rename(temp.Name(), filepath.Join(outputRoot, name)); err != nil {
		return "", 0, err
	}
	return name, info.Size(), nil
}
