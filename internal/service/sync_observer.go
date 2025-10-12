package service

import (
	"log"
)

type LogObserver struct{}

func (o *LogObserver) OnSyncStart(task *SyncTask) {
	log.Printf("开始同步表 %s -> %s", task.SourceTable, task.TargetTable)
}

func (o *LogObserver) OnSyncComplete(task *SyncTask) {
	log.Printf("表同步完成 %s -> %s", task.SourceTable, task.TargetTable)
}

func (o *LogObserver) OnSyncError(task *SyncTask, err error) {
	log.Printf("表同步错误 %s -> %s: %v", task.SourceTable, task.TargetTable, err)
}
