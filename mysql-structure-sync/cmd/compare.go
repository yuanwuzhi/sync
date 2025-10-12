package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mysql-structure-sync/internal/sync"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	sourceDB  string
	targetDB  string
	tableName string

	compareCmd = &cobra.Command{
		Use:   "compare",
		Short: "Compare table structures between two MySQL databases",
		Long:  `Compare table structures and generate SQL scripts to synchronize the target with the source.`,
		Run: func(cmd *cobra.Command, args []string) {
			logrus.Info("Starting table structure comparison")

			// Read config
			sourceDSN := viper.GetString(fmt.Sprintf("databases.%s.dsn", sourceDB))
			if sourceDSN == "" {
				logrus.Fatalf("Source database '%s' not found in config", sourceDB)
			}

			targetDSN := viper.GetString(fmt.Sprintf("databases.%s.dsn", targetDB))
			if targetDSN == "" {
				logrus.Fatalf("Target database '%s' not found in config", targetDB)
			}

			// Create comparer
			comparer, err := sync.NewComparer(sourceDSN, targetDSN)
			if err != nil {
				logrus.Fatalf("Failed to initialize comparer: %v", err)
			}
			defer comparer.Close()

			// Compare table structure and generate sync plan
			if tableName == "" {
				logrus.Info("未指定表名，将自动遍历源数据库所有表进行比较")

				// 检查是否启用合并输出
				mergeOutput := viper.GetBool("options.merge_output")
				if mergeOutput {
					logrus.Info("启用合并输出模式，所有表的差异将合并到单个文件中")
				}

				tableNames, err := comparer.GetAllTableNames(comparer.SourceDB)
				if err != nil {
					logrus.Fatalf("获取源数据库表名失败: %v", err)
				}

				if mergeOutput {
					// 合并输出模式
					var mergedPlan sync.MergedSyncPlan
					var allPlans []sync.SyncPlan
					totalDiffs := 0

					for _, tbl := range tableNames {
						plan, err := comparer.Compare(tbl)
						if err != nil {
							logrus.Errorf("表 %s 比较失败: %v", tbl, err)
							continue
						}
						if len(plan.Differences) == 0 {
							logrus.Infof("表 %s 结构完全一致，无需同步", tbl)
							continue // 只保留有差异的表
						}
						allPlans = append(allPlans, *plan)
						totalDiffs += len(plan.Differences)
						logrus.Infof("表 %s 比较完成: 共 %d 处差异", tbl, len(plan.Differences))
					}
					// 如果所有表都无差异，不生成任何文件
					if len(allPlans) == 0 {
						logrus.Infof("所有表结构一致，无需同步，不生成任何文件")
						return
					}
					// 构建合并计划
					mergedPlan.Tables = allPlans
					mergedPlan.TotalTables = len(allPlans)
					mergedPlan.TotalDiffs = totalDiffs
					mergedPlan.Status = "completed"
					// 生成合并输出文件
					timestamp := time.Now().Format("20060102_150405")
					jsonFilename := fmt.Sprintf("merged_sync_%s.json", timestamp)
					sqlFilename := fmt.Sprintf("merged_sync_%s.sql", timestamp)
					if err := mergedPlan.SaveJSON(jsonFilename); err != nil {
						logrus.Errorf("保存合并 JSON 失败: %v", err)
					} else {
						logrus.Infof("合并同步计划已保存到 %s", jsonFilename)
					}
					if err := mergedPlan.SaveSQL(sqlFilename); err != nil {
						logrus.Errorf("保存合并 SQL 失败: %v", err)
					} else {
						logrus.Infof("合并 SQL 脚本已保存到 %s", sqlFilename)
					}
					logrus.Infof("合并输出完成: 共 %d 个表，%d 处差异", mergedPlan.TotalTables, mergedPlan.TotalDiffs)
				} else {
					// 独立输出模式（原有逻辑）
					for _, tbl := range tableNames {
						plan, err := comparer.Compare(tbl)
						if err != nil {
							logrus.Errorf("表 %s 比较失败: %v", tbl, err)
							continue
						}

						// 检查是否有差异，如果没有差异则跳过文件生成
						if len(plan.Differences) == 0 {
							logrus.Infof("表 %s 结构完全一致，无需同步，跳过文件生成", tbl)
							continue
						}

						timestamp := time.Now().Format("20060102_150405")
						jsonFilename := fmt.Sprintf("%s_%s.json", tbl, timestamp)
						sqlFilename := fmt.Sprintf("%s_%s.sql", tbl, timestamp)
						if err := plan.SaveJSON(jsonFilename); err != nil {
							logrus.Errorf("表 %s 保存 JSON 失败: %v", tbl, err)
						} else {
							logrus.Infof("表 %s 的同步计划已保存到 %s", tbl, jsonFilename)
						}
						if err := plan.SaveSQL(sqlFilename); err != nil {
							logrus.Errorf("表 %s 保存 SQL 失败: %v", tbl, err)
						} else {
							logrus.Infof("表 %s 的 SQL 脚本已保存到 %s", tbl, sqlFilename)
						}
						logrus.Infof("表 %s 比较完成: 共 %d 处差异", tbl, len(plan.Differences))
					}
				}
				// 新增：对比目标库多余的表，并输出建表SQL
				sourceTableNames, err := comparer.GetAllTableNames(comparer.SourceDB)
				if err != nil {
					logrus.Fatalf("获取源数据库表名失败: %v", err)
				}
				targetTableNames, err := comparer.GetAllTableNames(comparer.TargetDB)
				if err != nil {
					logrus.Fatalf("获取目标数据库表名失败: %v", err)
				}
				sourceTableSet := make(map[string]struct{})
				for _, t := range sourceTableNames {
					sourceTableSet[t] = struct{}{}
				}
				var extraTables []string
				for _, t := range targetTableNames {
					if _, ok := sourceTableSet[t]; !ok {
						extraTables = append(extraTables, t)
					}
				}
				if len(extraTables) > 0 {
					logrus.Infof("目标库多余的表: %v", extraTables)
					for _, tbl := range extraTables {
						createSQL, err := comparer.GetCreateTableSQL(comparer.TargetDB, tbl)
						if err != nil {
							logrus.Errorf("获取表 %s 的建表SQL失败: %v", tbl, err)
							continue
						}
						logrus.Infof("表 %s 的建表SQL:\n%s", tbl, createSQL)
						filename := fmt.Sprintf("extra_%s_create_%s.sql", tbl, time.Now().Format("20060102_150405"))
						if err := writeStringToFile(filename, createSQL+";\n"); err != nil {
							logrus.Errorf("写入建表SQL到文件失败: %v", err)
						} else {
							logrus.Infof("表 %s 的建表SQL已保存到 %s", tbl, filename)
						}

						// 新增：用临时表名在源库创建表，自动对比结构
						tmpTable := "__tmp_sync_check_" + tbl
						tmpCreateSQL := createSQL
						// 替换表名为临时表名
						tmpCreateSQL = replaceTableNameInCreateSQL(createSQL, tbl, tmpTable)
						// 先删除临时表，防止冲突
						_ = comparer.DropTable(comparer.SourceDB, tmpTable)
						if err := comparer.CreateTable(comparer.SourceDB, tmpCreateSQL); err != nil {
							logrus.Errorf("用建表SQL在源库创建临时表失败: %v", err)
							continue
						}
						plan, err := comparer.Compare(tbl)
						if err != nil {
							logrus.Errorf("对比临时表和目标表结构失败: %v", err)
							_ = comparer.DropTable(comparer.SourceDB, tmpTable)
							continue
						}
						if len(plan.Differences) == 0 {
							logrus.Infof("表 %s 的建表SQL完全匹配，无需后续结构同步。", tbl)
						} else {
							logrus.Warnf("表 %s 的建表SQL与目标表结构仍有差异：", tbl)
							for i, diff := range plan.Differences {
								logrus.Warnf("%d. %s: %s", i+1, diff.Type, diff.Description)
							}
							logrus.Warnf("请手动调整建表SQL，确保结构完全一致！")
						}
						_ = comparer.DropTable(comparer.SourceDB, tmpTable)
					}
				} else {
					logrus.Infof("目标库没有多余的表")
				}
				return
			}

			// Compare table structure and generate sync plan
			plan, err := comparer.Compare(tableName)
			if err != nil {
				logrus.Fatalf("Comparison failed: %v", err)
			}

			// 检查是否有差异，如果没有差异则跳过文件生成
			if len(plan.Differences) == 0 {
				logrus.Infof("表 %s 结构完全一致，无需同步，跳过文件生成", tableName)
				return
			}

			// Generate output files with timestamp
			timestamp := time.Now().Format("20060102_150405")
			jsonFilename := fmt.Sprintf("%s_%s.json", tableName, timestamp)
			sqlFilename := fmt.Sprintf("%s_%s.sql", tableName, timestamp)

			if err := plan.SaveJSON(jsonFilename); err != nil {
				logrus.Errorf("Failed to save JSON plan: %v", err)
			} else {
				logrus.Infof("Saved sync plan to %s", jsonFilename)
			}

			if err := plan.SaveSQL(sqlFilename); err != nil {
				logrus.Errorf("Failed to save SQL script: %v", err)
			} else {
				logrus.Infof("Saved SQL script to %s", sqlFilename)
			}

			// Print summary
			logrus.Infof("Comparison completed: %d differences found", len(plan.Differences))
			for i, diff := range plan.Differences {
				logrus.Infof("%d. %s: %s", i+1, diff.Type, diff.Description)
			}
		},
	}
)

func init() {
	compareCmd.Flags().StringVar(&sourceDB, "source", "", "Source database name (from config)")
	compareCmd.Flags().StringVar(&targetDB, "target", "", "Target database name (from config)")
	compareCmd.Flags().StringVar(&tableName, "table", "", "Table name to compare (留空则对比所有表)")

	// Mark flags as required
	compareCmd.MarkFlagRequired("source")
	compareCmd.MarkFlagRequired("target")
}

func writeStringToFile(filename, content string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}

func replaceTableNameInCreateSQL(createSQL, oldName, newName string) string {
	// 只替换第一个出现的表名
	return strings.Replace(createSQL, "CREATE TABLE `"+oldName+"`", "CREATE TABLE `"+newName+"`", 1)
}
