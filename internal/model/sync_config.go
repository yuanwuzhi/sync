package model

type SyncConfig struct {
	ID             uint   `json:"id" gorm:"primaryKey"`
	SourceHost     string `json:"source_host"`
	SourcePort     int    `json:"source_port"`
	SourceUser     string `json:"source_user"`
	SourcePassword string `json:"source_password"`
	SourceDatabase string `json:"source_database"`
	SourceTable    string `json:"source_table"`

	TargetHost     string `json:"target_host"`
	TargetPort     int    `json:"target_port"`
	TargetUser     string `json:"target_user"`
	TargetPassword string `json:"target_password"`
	TargetDatabase string `json:"target_database"`
	TargetTable    string `json:"target_table"`

	LastSyncTime int64 `json:"last_sync_time"`
}
