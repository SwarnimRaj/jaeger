package badger

import (
	"golang.org/x/sys/unix"
)

func (f *Factory) diskStatisticsUpdate() {
	// These stats are not interesting with Windows as there's no separate tmpfs
	// In case of ephemeral these are the same, but we'll report them separately for consistency
	var keyDirStatfs unix.Statfs_t
	_ = unix.Statfs(f.Options.GetPrimary().KeyDirectory, &keyDirStatfs)

	var valDirStatfs unix.Statfs_t
	_ = unix.Statfs(f.Options.GetPrimary().ValueDirectory, &valDirStatfs)

	// Using Bavail instead of Bfree to get non-priviledged user space available
	ValueLogSpaceAvailable.Set(int64(valDirStatfs.Bavail) * valDirStatfs.Bsize)
	KeyLogSpaceAvailable.Set(int64(keyDirStatfs.Bavail) * keyDirStatfs.Bsize)

	/*
	 TODO If we wanted to clean up oldest data to free up diskspace, we need at a minimum an index to the StartTime
	 Additionally to that, the deletion might not save anything if the ratio of removed values is lower than the RunValueLogGC's deletion ratio
	 and with the keys the LSM compaction must remove the offending files also. Thus, there's no guarantee the clean up would
	 actually reduce the amount of diskspace used any faster than allowing TTL to remove them.
	*/
}
