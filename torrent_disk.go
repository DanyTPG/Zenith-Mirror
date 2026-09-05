package main

import (
	"fmt"
	"github.com/shirou/gopsutil/v3/disk"
)

func checkDiskSpaceImpl(p string, need int64) error {
	u, err := disk.Usage(p)
	if err != nil {
		return nil // don't block if we can't stat
	}
	if u.Free < uint64(need)+256*1024*1024 {
		return fmt.Errorf("not enough disk space: need %s, free %s", FormatBytes(need), FormatBytes(int64(u.Free)))
	}
	return nil
}
