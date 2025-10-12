package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync/internal/config"
	"sync/internal/service"
)

func main() {
	// 使用相对路径加载配置文件
	configPath := filepath.Join("..", "configs", "config.yml")
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatal("加载配置失败:", err)
	}

	syncService, err := service.NewSyncService(cfg)
	if err != nil {
		log.Fatal(err)
	}
	syncService.RegisterObserver(&service.LogObserver{})

	// 启动同步服务
	ctx := context.Background()
	fmt.Println("mysql-sync 启动成功 🚗🚀")
	if err := syncService.StartSync(ctx); err != nil {
		log.Fatal(err)
	}

}
