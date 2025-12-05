package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/internal/config"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// ColumnDetail 用于存储字段详细信息 (新增结构体，用于同步表结构)
type ColumnDetail struct {
	ColumnName    string         `gorm:"column:COLUMN_NAME"`
	ColumnType    string         `gorm:"column:COLUMN_TYPE"`
	IsNullable    string         `gorm:"column:IS_NULLABLE"`
	ColumnDefault sql.NullString `gorm:"column:COLUMN_DEFAULT"`
	Extra         string         `gorm:"column:EXTRA"`
	ColumnComment string         `gorm:"column:COLUMN_COMMENT"`
}

// SyncTask 定义单个同步任务
type SyncTask struct {
	SourceTable  string
	TargetTable  string
	LastSyncTime int64
	BatchSize    int
	Status       string
	Error        error
	mutex        sync.RWMutex
}

// SyncService 同步服务
type SyncService struct {
	sourceDB  *gorm.DB
	targetDB  *gorm.DB
	config    *config.Config
	tasks     map[string]*SyncTask // key: sourceTable
	observers []SyncObserver
	ctx       context.Context
	cancel    context.CancelFunc
	mutex     sync.RWMutex
}

// SyncObserver 同步观察者接口
type SyncObserver interface {
	OnSyncStart(task *SyncTask)
	OnSyncComplete(task *SyncTask)
	OnSyncError(task *SyncTask, err error)
}

// NewSyncService 创建同步服务
func NewSyncService(cfg *config.Config) (*SyncService, error) {
	sourceDB, err := initDB(cfg.Database.Source.GetDSN())
	if err != nil {
		return nil, fmt.Errorf("初始化源数据库失败: %w", err)
	}

	targetDB, err := initDB(cfg.Database.Target.GetDSN())
	if err != nil {
		return nil, fmt.Errorf("初始化目标数据库失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	service := &SyncService{
		sourceDB: sourceDB,
		targetDB: targetDB,
		config:   cfg,
		tasks:    make(map[string]*SyncTask),
		ctx:      ctx,
		cancel:   cancel,
	}

	// 初始化同步任务
	for _, pair := range cfg.Sync.TablePairs {
		service.AddSyncTask(pair.Source, pair.Target)
	}

	return service, nil
}

// AddSyncTask 添加同步任务
func (s *SyncService) AddSyncTask(sourceTable, targetTable string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.tasks[sourceTable] = &SyncTask{
		SourceTable:  sourceTable,
		TargetTable:  targetTable,
		BatchSize:    s.config.Sync.BatchSize,
		Status:       "ready",
		LastSyncTime: 0, // 设置为 0，表示不限制时间
	}
}

// RegisterObserver 注册观察者
func (s *SyncService) RegisterObserver(observer SyncObserver) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.observers = append(s.observers, observer)
}

// StartSync 开始同步
func (s *SyncService) StartSync(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(s.config.Sync.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.syncAll()
		}
	}
}

// syncAll 同步所有表
func (s *SyncService) syncAll() {
	var wg sync.WaitGroup
	for _, task := range s.tasks {
		wg.Add(1)
		go func(t *SyncTask) {
			defer wg.Done()
			s.syncTable(t)
		}(task)
	}
	wg.Wait()
}

// syncTable 同步单个表
func (s *SyncService) syncTable(task *SyncTask) {
	s.notifyStart(task)

	// ==========================================
	// [新增] 步骤：同步表结构 (Schema Sync)
	// 在获取数据前，先检查并修复目标表缺失的字段
	// ==========================================
	if err := s.syncTableSchema(task); err != nil {
		s.notifyError(task, fmt.Errorf("同步表结构失败: %w", err))
		return
	}

	// 获取表的所有字段
	columns, err := s.getAllColumns(s.sourceDB, task.SourceTable)
	if err != nil {
		s.notifyError(task, err)
		return
	}

	// 判断是否需要同步
	needSync, err := s.needSync(task, columns)
	if err != nil {
		s.notifyError(task, err)
		return
	}

	if !needSync {
		task.mutex.Lock()
		task.Status = "completed"
		task.mutex.Unlock()
		s.notifyComplete(task)
		return
	}

	// 获取源表总记录数
	var totalCount int64
	if err := s.sourceDB.Table(task.SourceTable).Count(&totalCount).Error; err != nil {
		s.notifyError(task, err)
		return
	}

	// 计算总页数
	batchSize := task.BatchSize
	totalPages := int(math.Ceil(float64(totalCount) / float64(batchSize)))

	// 分页处理数据
	for page := 0; page < totalPages; page++ {
		offset := page * batchSize
		var sourceRecords []map[string]interface{}

		// 根据 sync_mode 决定同步方式
		if s.config.Sync.SyncMode == "full" {
			// 全量同步
			if err := s.sourceDB.Table(task.SourceTable).
				Offset(offset).
				Limit(batchSize).
				Find(&sourceRecords).Error; err != nil {
				s.notifyError(task, err)
				return
			}
		} else {
			// 增量同步
			tablePair := s.getTableConfig(task.SourceTable)
			if tablePair.CheckMethod == "update_time" && tablePair.UpdateField != "" {
				// 获取目标表中最后更新的时间
				var lastTargetUpdate time.Time
				if err := s.targetDB.Table(task.TargetTable).
					Select(tablePair.UpdateField).
					Order(tablePair.UpdateField + " DESC").
					Limit(1).
					Scan(&lastTargetUpdate).Error; err != nil {
					s.notifyError(task, err)
					return
				}

				// 只获取源表中更新时间大于目标表最后更新时间的记录
				if err := s.sourceDB.Table(task.SourceTable).
					Where(fmt.Sprintf("%s > ?", tablePair.UpdateField), lastTargetUpdate).
					Offset(offset).
					Limit(batchSize).
					Find(&sourceRecords).Error; err != nil {
					s.notifyError(task, err)
					return
				}
			}
		}

		// 同步当前页的数据
		if err := s.syncBatchData(task.TargetTable, sourceRecords); err != nil {
			s.notifyError(task, err)
			return
		}

		log.Printf("已同步表 %s 的第 %d/%d 页数据", task.SourceTable, page+1, totalPages)
	}

	// 删除目标表中不存在于源表的记录
	if err := s.cleanupTargetTable(task.SourceTable, task.TargetTable, columns); err != nil {
		s.notifyError(task, fmt.Errorf("清理目标表失败: %w", err))
		return
	}

	task.mutex.Lock()
	task.Status = "completed"
	task.mutex.Unlock()
	s.notifyComplete(task)
}

// [新增] syncTableSchema 对比源表和目标表的结构，自动添加目标表缺失的字段
func (s *SyncService) syncTableSchema(task *SyncTask) error {
	// 1. 获取源表详细字段信息
	sourceCols, err := s.getColumnDetails(s.sourceDB, task.SourceTable)
	if err != nil {
		return fmt.Errorf("获取源表结构失败: %w", err)
	}

	// 2. 获取目标表所有字段名 (为了快速查找是否存在)
	targetColNames, err := s.getAllColumns(s.targetDB, task.TargetTable)
	if err != nil {
		return fmt.Errorf("获取目标表结构失败: %w", err)
	}

	targetColMap := make(map[string]bool)
	for _, name := range targetColNames {
		targetColMap[name] = true
	}

	// 3. 遍历源表字段，检查目标表是否缺失
	for _, col := range sourceCols {
		if !targetColMap[col.ColumnName] {
			log.Printf("检测到表 %s 在目标库缺失字段: %s (%s)，正在自动修复...", task.TargetTable, col.ColumnName, col.ColumnType)

			// 构建 ALTER TABLE 语句
			// 示例: ALTER TABLE `mytable` ADD COLUMN `new_col` varchar(255) DEFAULT NULL COMMENT 'xxx'
			sql := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", task.TargetTable, col.ColumnName, col.ColumnType)

			// 处理 NOT NULL
			if col.IsNullable == "NO" {
				sql += " NOT NULL"
			} else {
				sql += " NULL"
			}

			// 处理默认值
			if col.ColumnDefault.Valid {
				sql += fmt.Sprintf(" DEFAULT '%s'", col.ColumnDefault.String)
			}

			// 处理注释
			if col.ColumnComment != "" {
				escapedComment := strings.ReplaceAll(col.ColumnComment, "'", "\\'")
				sql += fmt.Sprintf(" COMMENT '%s'", escapedComment)
			}

			// 执行 DDL
			if err := s.targetDB.Exec(sql).Error; err != nil {
				log.Printf("尝试添加字段失败: %v, SQL: %s", err, sql)
				return err
			}
			log.Printf("成功添加字段: %s 到表 %s", col.ColumnName, task.TargetTable)
		}
	}
	return nil
}

// [新增] getColumnDetails 获取表字段的详细信息（用于生成DDL）
func (s *SyncService) getColumnDetails(db *gorm.DB, tableName string) ([]ColumnDetail, error) {
	var details []ColumnDetail
	// 注意：information_schema 查询需要指定 TABLE_SCHEMA = DATABASE() 避免查到其他库同名表
	err := db.Raw(`
		SELECT 
			COLUMN_NAME, 
			COLUMN_TYPE, 
			IS_NULLABLE, 
			COLUMN_DEFAULT, 
			EXTRA, 
			COLUMN_COMMENT 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = DATABASE() 
		AND TABLE_NAME = ? 
		ORDER BY ORDINAL_POSITION`, tableName).Scan(&details).Error

	if err != nil {
		return nil, err
	}
	return details, nil
}

// 同步批量数据
func (s *SyncService) syncBatchData(table string, records []map[string]interface{}) error {
	// 定义重试策略
	const (
		retryCount    = 3
		baseDelay     = 100 * time.Millisecond
		maxRetryDelay = 2 * time.Second
	)

	return s.targetDB.Transaction(func(tx *gorm.DB) error {
		// 在事务开始时关闭外键检查，防止 Error 1452 并发死锁
		if err := tx.Exec("SET FOREIGN_KEY_CHECKS = 0").Error; err != nil {
			log.Printf("警告: 无法关闭外键检查: %v", err)
		}

		// 1. 空记录检查
		if len(records) == 0 {
			return nil
		}

		// 2. 使用指数退避的重试机制执行SQL
		var lastErr error
		for attempt := 0; attempt < retryCount; attempt++ {
			for _, record := range records {
				if err := s.syncSingleRecord(tx, table, record); err != nil {
					lastErr = err
					// 计算延迟时间（指数退避）
					delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt)))
					if delay > maxRetryDelay {
						delay = maxRetryDelay
					}
					log.Printf("同步记录失败，第 %d 次重试，等待 %v: %v", attempt+1, delay, err)
					time.Sleep(delay)
					break // 退出当前记录的循环，进行重试
				}
			}
			if lastErr == nil {
				log.Printf("成功同步 %d 条记录到表 %s", len(records), table)
				return nil
			}
		}

		return fmt.Errorf("批量同步失败，已重试 %d 次: %w", retryCount, lastErr)
	})
}

// 同步单条记录
func (s *SyncService) syncSingleRecord(tx *gorm.DB, table string, record map[string]interface{}) error {
	// 构建字段名和值的列表
	var columns []string
	var placeholders []string
	var values []interface{}
	var updates []string

	for col, val := range record {
		columns = append(columns, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
		updates = append(updates, fmt.Sprintf("`%s` = VALUES(`%s`)", col, col))
	}

	// 构建 INSERT ... ON DUPLICATE KEY UPDATE 语句
	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updates, ", "))

	// 执行 SQL
	if err := tx.Exec(sql, values...).Error; err != nil {
		return fmt.Errorf("更新记录失败: %w", err)
	}

	log.Printf("同步记录成功: %v", record)
	return nil
}

// --- 动态获取表的主键名 ---
func (s *SyncService) getPrimaryKey(db *gorm.DB, tableName string) (string, error) {
	var primaryKey string
	// 查询 INFORMATION_SCHEMA 获取主键名
	err := db.Raw(`
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		AND TABLE_NAME = ?
		AND COLUMN_KEY = 'PRI'
		LIMIT 1`, tableName).Scan(&primaryKey).Error

	if err != nil {
		return "", err
	}
	if primaryKey == "" {
		// 如果没找到主键，默认回退到 id，但大概率会报错
		return "id", nil
	}
	return primaryKey, nil
}

// 添加清理目标表的方法
func (s *SyncService) cleanupTargetTable(sourceTable, targetTable string, columns []string) error {
	// 获取主键字段名 (动态获取，不再写死 "id")
	primaryKey, err := s.getPrimaryKey(s.targetDB, targetTable)
	if err != nil {
		return fmt.Errorf("获取主键失败: %w", err)
	}
	if primaryKey == "id" {
		log.Printf("提示: 未在表 %s 中找到显式主键，尝试使用 'id' 进行清理", targetTable)
	}

	// 使用事务包裹清理逻辑，并关闭外键检查
	return s.targetDB.Transaction(func(tx *gorm.DB) error {
		// 关闭外键检查，防止删除时因外键约束失败
		if err := tx.Exec("SET FOREIGN_KEY_CHECKS = 0").Error; err != nil {
			log.Printf("警告: 清理时无法关闭外键检查: %v", err)
		}

		// 检查目标表是否为空
		var count int64
		if err := tx.Table(targetTable).Count(&count).Error; err != nil {
			return fmt.Errorf("检查目标表记录数失败: %w", err)
		}

		// 如果目标表为空，则不需要清理
		if count == 0 {
			log.Printf("目标表 `%s` 为空，无需清理", targetTable)
			return nil
		}

		// 创建临时表来存储需要保留的记录
		tempTable := fmt.Sprintf("temp_%s_%d", targetTable, time.Now().UnixNano())

		// 创建临时表
		createTempTableSQL := fmt.Sprintf("CREATE TEMPORARY TABLE `%s` LIKE `%s`", tempTable, targetTable)
		if err := tx.Exec(createTempTableSQL).Error; err != nil {
			return fmt.Errorf("创建临时表失败: %w", err)
		}

		// 将源表数据复制到临时表
		copyDataSQL := fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`", tempTable, sourceTable)
		if err := tx.Exec(copyDataSQL).Error; err != nil {
			return fmt.Errorf("复制数据到临时表失败: %w", err)
		}

		// 构建主键条件（动态使用 primaryKey）
		joinCondition := fmt.Sprintf("t1.`%s` = t2.`%s`", primaryKey, primaryKey)

		// 删除目标表中不在临时表中的记录
		deleteSQL := fmt.Sprintf("DELETE t1 FROM `%s` t1 LEFT JOIN `%s` t2 ON %s WHERE t2.`%s` IS NULL",
			targetTable, tempTable, joinCondition, primaryKey)

		if result := tx.Exec(deleteSQL); result.Error != nil {
			return fmt.Errorf("清理目标表失败: %w", result.Error)
		} else if result.RowsAffected > 0 {
			log.Printf("已从目标表删除 %d 条不存在的记录", result.RowsAffected)
		}

		// 删除临时表
		dropTempTableSQL := fmt.Sprintf("DROP TEMPORARY TABLE IF EXISTS `%s`", tempTable)
		if err := tx.Exec(dropTempTableSQL).Error; err != nil {
			log.Printf("删除临时表失败: %v", err)
		}

		return nil
	})
}

// 通知方法
func (s *SyncService) notifyStart(task *SyncTask) {
	for _, observer := range s.observers {
		observer.OnSyncStart(task)
	}
}

func (s *SyncService) notifyComplete(task *SyncTask) {
	for _, observer := range s.observers {
		observer.OnSyncComplete(task)
	}
}

func (s *SyncService) notifyError(task *SyncTask, err error) {
	task.mutex.Lock()
	task.Error = err
	task.Status = "error"
	task.mutex.Unlock()

	for _, observer := range s.observers {
		observer.OnSyncError(task, err)
	}
}

// Stop 停止同步服务
func (s *SyncService) Stop() {
	s.cancel()
}

// initDB 初始化数据库连接
func initDB(dsn string) (*gorm.DB, error) {
	// 添加 sql_mode 参数来允许无效日期
	if !strings.Contains(dsn, "?") {
		dsn += "?"
	} else {
		dsn += "&"
	}
	dsn += "sql_mode='ALLOW_INVALID_DATES'"

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
	})
	if err != nil {
		return nil, err
	}

	// 设置连接池
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxIdleConns(10)           // 设置空闲连接池中的最大连接数
	sqlDB.SetMaxOpenConns(100)          // 设置打开数据库连接的最大数量
	sqlDB.SetConnMaxLifetime(time.Hour) // 设置连接可复用的最大时间

	return db, nil
}

// 添加获取所有字段的方法
func (s *SyncService) getAllColumns(db *gorm.DB, tableName string) ([]string, error) {
	var columns []string

	err := db.Raw(`
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`, tableName).Scan(&columns).Error

	if err != nil {
		return nil, fmt.Errorf("获取字段列表失败: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("表 %s 没有任何字段", tableName)
	}

	return columns, nil
}

// 添加比较表数据的方法
func (s *SyncService) needSync(task *SyncTask, columns []string) (bool, error) {
	// 获取表配置
	tablePair := s.getTableConfig(task.SourceTable)

	switch tablePair.CheckMethod {
	case "update_time":
		if tablePair.UpdateField == "" {
			log.Printf("警告: 表 %s 配置使用update_time检查但未指定更新时间字段，将使用checksum", task.SourceTable)
			return s.checkByChecksum(task.SourceTable, task.TargetTable)
		}
		return s.checkByUpdateTime(task.SourceTable, task.TargetTable, tablePair.UpdateField)

	case "count":
		return s.checkByCount(task.SourceTable, task.TargetTable)

	case "checksum":
		fallthrough
	default:
		return s.checkByChecksum(task.SourceTable, task.TargetTable)
	}
}

func (s *SyncService) checkByUpdateTime(sourceTable, targetTable, updateField string) (bool, error) {
	// 检查字段是否存在
	columns, err := s.getAllColumns(s.sourceDB, sourceTable)
	if err != nil {
		return true, err
	}

	hasUpdateField := false
	for _, col := range columns {
		if col == updateField {
			hasUpdateField = true
			break
		}
	}

	if !hasUpdateField {
		log.Printf("警告: 表 %s 不存在更新时间字段 %s，将使用checksum", sourceTable, updateField)
		return s.checkByChecksum(sourceTable, targetTable)
	}

	// 比较最新更新时间
	var sourceLastUpdate, targetLastUpdate time.Time
	if err := s.sourceDB.Table(sourceTable).Select(updateField).Order(updateField + " DESC").Limit(1).Scan(&sourceLastUpdate).Error; err != nil {
		return true, err
	}
	if err := s.targetDB.Table(targetTable).Select(updateField).Order(updateField + " DESC").Limit(1).Scan(&targetLastUpdate).Error; err != nil {
		return true, err
	}

	return !sourceLastUpdate.Equal(targetLastUpdate), nil
}

func (s *SyncService) checkByCount(sourceTable, targetTable string) (bool, error) {
	var sourceCount, targetCount int64
	if err := s.sourceDB.Table(sourceTable).Count(&sourceCount).Error; err != nil {
		return true, fmt.Errorf("获取源表记录数失败: %w", err)
	}
	if err := s.targetDB.Table(targetTable).Count(&targetCount).Error; err != nil {
		return true, fmt.Errorf("获取目标表记录数失败: %w", err)
	}

	if sourceCount != targetCount {
		log.Printf("表 %s 记录数不一致: 源表=%d, 目标表=%d", sourceTable, sourceCount, targetCount)
		return true, nil
	}

	log.Printf("表 %s 记录数一致", sourceTable)
	return false, nil
}

func (s *SyncService) checkByChecksum(sourceTable, targetTable string) (bool, error) {
	// 定义结构体来接收结果
	type ChecksumResult struct {
		Table    string
		Checksum int64
	}

	var sourceResult, targetResult ChecksumResult

	// 获取源表校验和
	if err := s.sourceDB.Raw("CHECKSUM TABLE " + sourceTable).Scan(&sourceResult).Error; err != nil {
		return true, fmt.Errorf("获取源表校验和失败: %w", err)
	}

	// 获取目标表校验和
	if err := s.targetDB.Raw("CHECKSUM TABLE " + targetTable).Scan(&targetResult).Error; err != nil {
		return true, fmt.Errorf("获取目标表校验和失败: %w", err)
	}

	if sourceResult.Checksum != targetResult.Checksum {
		log.Printf("表 %s 数据校验和不一致: 源表=%d, 目标表=%d",
			sourceTable, sourceResult.Checksum, targetResult.Checksum)
		return true, nil
	}

	log.Printf("表 %s 数据一致，无需同步", sourceTable)
	return false, nil
}

// 获取表配置
func (s *SyncService) getTableConfig(sourceTable string) *config.TablePair {
	// 遍历配置中的表配置
	for _, pair := range s.config.Sync.TablePairs {
		if pair.Source == sourceTable {
			return &pair
		}
	}

	// 如果没有找到对应配置，返回默认配置
	return &config.TablePair{
		Source:      sourceTable,
		Target:      sourceTable,
		CheckMethod: "checksum", // 默认使用 checksum 检查
	}
}
