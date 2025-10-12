package service

import (
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"math/rand"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// ----------------------------- 模拟配置结构 -----------------------------

type DBConnection struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

type TablePair struct {
	Source      string
	Target      string
	CheckMethod string
	UpdateField string
}

type SyncConfig struct {
	Interval   int
	BatchSize  int
	SyncMode   string
	TablePairs []TablePair
}

type DatabaseConfig struct {
	Source DBConnection
	Target DBConnection
}

type Config struct {
	Database DatabaseConfig
	Sync     SyncConfig
}

// ----------------------------- 核心服务结构 -----------------------------

type SyncServiceBenchMark struct {
	db  *gorm.DB
	cfg *Config
}

func NewSyncServiceTest(cfg *Config, db *gorm.DB) (*SyncServiceBenchMark, error) {
	return &SyncServiceBenchMark{
		db:  db,
		cfg: cfg,
	}, nil
}

func (s *SyncServiceBenchMark) syncAllSyncServiceBenchMark() {
	// 获取需要更新的记录
	var records []map[string]interface{}
	s.db.Raw("SELECT * FROM user_records WHERE updated_at > ?", time.Now().Add(-24*time.Hour)).Scan(&records)

	// 批量更新记录
	for _, record := range records {
		s.db.Exec("INSERT INTO user_records (name, email, age, gender, phone, address, status, created_by, updated_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			record["name"],
			record["email"],
			record["age"],
			record["gender"],
			record["phone"],
			record["address"],
			record["status"],
			record["created_by"],
			record["updated_by"],
			record["created_at"],
			record["updated_at"],
		)
	}
}

// ----------------------------- 工具函数 -----------------------------

func generateMockUserRecords(count int) []map[string]interface{} {
	records := make([]map[string]interface{}, count)
	now := time.Now()
	for i := 0; i < count; i++ {
		records[i] = map[string]interface{}{
			"name":       fmt.Sprintf("User%d", i),
			"email":      fmt.Sprintf("user%d@example.com", i),
			"age":        rand.Intn(50) + 18,
			"gender":     []string{"male", "female"}[rand.Intn(2)],
			"phone":      fmt.Sprintf("138%08d", i),
			"address":    fmt.Sprintf("Address %d", i),
			"status":     1,
			"created_by": "system",
			"updated_by": "system",
			"created_at": now.Add(-time.Duration(rand.Intn(30)) * 24 * time.Hour),
			"updated_at": now.Add(-time.Duration(rand.Intn(30)) * 24 * time.Hour),
		}
	}
	return records
}

func updateRandomRecords(records []map[string]interface{}) {
	updateCount := len(records) / 10
	now := time.Now()
	for i := 0; i < updateCount; i++ {
		idx := rand.Intn(len(records))
		records[idx]["phone"] = fmt.Sprintf("139%08d", rand.Intn(100000000))
		records[idx]["updated_at"] = now.Add(time.Duration(rand.Intn(24)) * time.Hour)
	}
}

func setupMockTables(mock sqlmock.Sqlmock, db *gorm.DB) {
	// 创建表的 SQL
	createTableSQL := `CREATE TABLE IF NOT EXISTS user_records (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(100) UNIQUE,
		age INT,
		gender VARCHAR(10),
		phone VARCHAR(20),
		address VARCHAR(255),
		status TINYINT DEFAULT 1,
		created_by VARCHAR(50),
		updated_by VARCHAR(50),
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`

	// 使用正则表达式匹配 SQL 语句
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS user_records.*").
		WillReturnResult(sqlmock.NewResult(0, 0))

	db.Exec(createTableSQL)
}

// ----------------------------- 性能测试 -----------------------------

func BenchmarkSyncService(b *testing.B) {
	dataSizes := []int{3000, 5000}

	for _, size := range dataSizes {
		b.Run(fmt.Sprintf("DataSize_%d", size), func(b *testing.B) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				b.Fatalf("创建模拟数据库失败: %v", err)
			}
			defer mockDB.Close()

			dialector := mysql.New(mysql.Config{
				Conn:                      mockDB,
				SkipInitializeWithVersion: true,
			})

			db, err := gorm.Open(dialector, &gorm.Config{})
			if err != nil {
				b.Fatalf("打开数据库失败: %v", err)
			}

			// 设置模拟表
			setupMockTables(mock, db)

			// 生成测试数据
			records := generateMockUserRecords(size)
			updateRandomRecords(records)

			// 模拟查询操作
			rows := sqlmock.NewRows([]string{"id", "name", "email", "age", "gender", "phone", "address", "status", "created_by", "updated_by", "created_at", "updated_at"})
			for _, record := range records {
				rows.AddRow(
					rand.Int63(),
					record["name"],
					record["email"],
					record["age"],
					record["gender"],
					record["phone"],
					record["address"],
					record["status"],
					record["created_by"],
					record["updated_by"],
					record["created_at"],
					record["updated_at"],
				)
			}

			// 设置查询期望
			mock.ExpectQuery("SELECT \\* FROM user_records WHERE updated_at > \\?").
				WithArgs(sqlmock.AnyArg()).
				WillReturnRows(rows)

			// 设置插入期望
			for i := 0; i < len(records); i++ {
				mock.ExpectExec("INSERT INTO user_records").
					WithArgs(
						sqlmock.AnyArg(), // name
						sqlmock.AnyArg(), // email
						sqlmock.AnyArg(), // age
						sqlmock.AnyArg(), // gender
						sqlmock.AnyArg(), // phone
						sqlmock.AnyArg(), // address
						sqlmock.AnyArg(), // status
						sqlmock.AnyArg(), // created_by
						sqlmock.AnyArg(), // updated_by
						sqlmock.AnyArg(), // created_at
						sqlmock.AnyArg(), // updated_at
					).
					WillReturnResult(sqlmock.NewResult(int64(i+1), 1))
			}

			cfg := &Config{
				Database: DatabaseConfig{
					Source: DBConnection{},
					Target: DBConnection{},
				},
				Sync: SyncConfig{
					Interval:  60,
					BatchSize: 1000,
					SyncMode:  "incremental",
					TablePairs: []TablePair{
						{
							Source:      "user_records",
							Target:      "user_records",
							CheckMethod: "update_time",
							UpdateField: "updated_at",
						},
					},
				},
			}

			service, err := NewSyncServiceTest(cfg, db)
			if err != nil {
				b.Fatalf("创建同步服务失败: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				service.syncAllSyncServiceBenchMark()
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				b.Errorf("未满足的数据库期望: %v", err)
			}
		})
	}
}

// ----------------------------- 功能测试 -----------------------------

func TestSyncServicePerformance(t *testing.T) {
	dataSizes := []int{3000, 5000, 8000, 10000, 20000}

	for _, size := range dataSizes {
		t.Run(fmt.Sprintf("DataSize_%d", size), func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("创建模拟数据库失败: %v", err)
			}
			defer mockDB.Close()

			dialector := mysql.New(mysql.Config{
				Conn:                      mockDB,
				SkipInitializeWithVersion: true,
			})

			db, err := gorm.Open(dialector, &gorm.Config{})
			if err != nil {
				t.Fatalf("打开数据库失败: %v", err)
			}

			// 设置模拟表
			setupMockTables(mock, db)

			// 生成测试数据
			records := generateMockUserRecords(size)
			updateRandomRecords(records)

			// 模拟查询操作
			rows := sqlmock.NewRows([]string{"id", "name", "email", "age", "gender", "phone", "address", "status", "created_by", "updated_by", "created_at", "updated_at"})
			for _, record := range records {
				rows.AddRow(
					rand.Int63(),
					record["name"],
					record["email"],
					record["age"],
					record["gender"],
					record["phone"],
					record["address"],
					record["status"],
					record["created_by"],
					record["updated_by"],
					record["created_at"],
					record["updated_at"],
				)
			}

			// 设置查询期望
			mock.ExpectQuery("SELECT \\* FROM user_records WHERE updated_at > \\?").
				WithArgs(sqlmock.AnyArg()).
				WillReturnRows(rows)

			// 设置插入期望
			for i := 0; i < len(records); i++ {
				mock.ExpectExec("INSERT INTO user_records").
					WithArgs(
						sqlmock.AnyArg(), // name
						sqlmock.AnyArg(), // email
						sqlmock.AnyArg(), // age
						sqlmock.AnyArg(), // gender
						sqlmock.AnyArg(), // phone
						sqlmock.AnyArg(), // address
						sqlmock.AnyArg(), // status
						sqlmock.AnyArg(), // created_by
						sqlmock.AnyArg(), // updated_by
						sqlmock.AnyArg(), // created_at
						sqlmock.AnyArg(), // updated_at
					).
					WillReturnResult(sqlmock.NewResult(int64(i+1), 1))
			}

			cfg := &Config{
				Database: DatabaseConfig{
					Source: DBConnection{},
					Target: DBConnection{},
				},
				Sync: SyncConfig{
					Interval:  60,
					BatchSize: 1000,
					SyncMode:  "incremental",
					TablePairs: []TablePair{
						{
							Source:      "user_records",
							Target:      "user_records",
							CheckMethod: "update_time",
							UpdateField: "updated_at",
						},
					},
				},
			}

			service, err := NewSyncServiceTest(cfg, db)
			if err != nil {
				t.Fatalf("创建同步服务失败: %v", err)
			}

			start := time.Now()
			service.syncAllSyncServiceBenchMark()
			duration := time.Since(start)

			t.Logf("数据量: %d, 耗时: %v, 平均每条: %v", size, duration, duration/time.Duration(size))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("未满足的数据库期望: %v", err)
			}
		})
	}
}
