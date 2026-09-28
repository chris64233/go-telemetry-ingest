package gotelemetryingest

import "os"

// syncDir fsync 一个目录，保证其中的 rename/truncate 元数据落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
