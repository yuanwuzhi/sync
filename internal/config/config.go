package config

import (
	"fmt"
	"github.com/spf13/viper"
	"strings"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Sync     SyncConfig     `mapstructure:"sync"`
}

type ServerConfig struct {
	Port int    `mapstructure:"port"`
	Host string `mapstructure:"host"`
}

type DatabaseConfig struct {
	Source DBConnection `mapstructure:"source"`
	Target DBConnection `mapstructure:"target"`
}

type DBConnection struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
}

type SyncConfig struct {
	BatchSize  int         `mapstructure:"batch_size"`
	Interval   int         `mapstructure:"interval"`
	SyncMode   string      `mapstructure:"sync_mode"`
	TablePairs []TablePair `mapstructure:"table_pairs"`
}

type TablePair struct {
	Source      string `mapstructure:"source"`
	Target      string `mapstructure:"target"`
	CheckMethod string `mapstructure:"check_method"`
	UpdateField string `mapstructure:"update_field"`
}

func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	// 基本配置
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	// 环境变量配置
	v.SetEnvPrefix("APP")                              // 环境变量前缀 APP_
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // 支持嵌套配置 使用 _ 分隔
	v.AutomaticEnv()                                   // 自动读取环境变量

	// 设置默认值
	setDefaults(v)

	// 读取配置文件
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	config := &Config{}
	if err := v.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}

	// 验证配置
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	return config, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.port", 28081)
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("sync.batch_size", 100)
	v.SetDefault("sync.interval", 60)
}

func validateConfig(cfg *Config) error {
	// 验证必要的配置项
	if cfg.Database.Source.Password == "" {
		return fmt.Errorf("source database password is required")
	}
	if cfg.Database.Target.Password == "" {
		return fmt.Errorf("target database password is required")
	}

	// 验证同步配置
	if cfg.Sync.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be greater than 0")
	}
	if cfg.Sync.Interval <= 0 {
		return fmt.Errorf("sync interval must be greater than 0")
	}

	// 添加表配置验证
	for _, pair := range cfg.Sync.TablePairs {
		if pair.Source == "" || pair.Target == "" {
			return fmt.Errorf("table pair source and target must not be empty")
		}

		if pair.CheckMethod == "update_time" && pair.UpdateField == "" {
			return fmt.Errorf("update_field is required when check_method is update_time")
		}

		if pair.CheckMethod != "checksum" &&
			pair.CheckMethod != "count" &&
			pair.CheckMethod != "update_time" {
			return fmt.Errorf("invalid check_method: %s", pair.CheckMethod)
		}
	}

	return nil
}

// GetDSN 返回数据库连接字符串
func (d *DBConnection) GetDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True",
		d.User, d.Password, d.Host, d.Port, d.Database)
}
