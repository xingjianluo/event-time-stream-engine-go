package engine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// SaveCheckpointFile 把检查点原子写入 path（临时文件 + fsync + 改名），
// 进程在写入中途退出不会留下半个检查点。
func SaveCheckpointFile(path string, cp Checkpoint) error {
	data, err := EncodeCheckpoint(cp)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create checkpoint temp file: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil { // fsync：确保字节落到磁盘再改名
		return fmt.Errorf("fsync checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("commit checkpoint: %w", err)
	}
	ok = true
	// fsync 目录，保证改名本身持久化。
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// LoadCheckpointFile 从 path 读取检查点。
// 文件不存在返回 ErrNoCheckpoint；内容无法识别返回 ErrCheckpointCorrupt 或
// ErrUnsupportedVersion（均被 wrap，可用 errors.Is 判断）。
func LoadCheckpointFile(path string) (Checkpoint, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Checkpoint{}, fmt.Errorf("%w: %s", ErrNoCheckpoint, path)
		}
		return Checkpoint{}, fmt.Errorf("open checkpoint: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("read checkpoint: %w", err)
	}
	cp, err := DecodeCheckpoint(data)
	if err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}
