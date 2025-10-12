```markdown
# MySQL 表结构对比与同步工具

一款专业的MySQL表结构比对与同步工具，可精确识别不同数据库间的列、索引、主键等差异，并生成安全的SQL同步脚本。

## 功能特性

- **精准结构对比**：比对列、索引、主键等表属性
- **最小化SQL生成**：仅生成必要的同步SQL语句
- **结构化输出**：同时生成JSON报告和带时间戳的SQL脚本
- **安全操作**：所有变更都封装在事务中确保安全
- **配置选项**：通过配置文件自定义比对行为

## 环境要求

- Go 1.20 或更高版本
- 源数据库和目标数据库的访问权限

## 安装指南

1. 克隆代码库：

```bash
git clone https://github.com/yourusername/mysql-structure-sync.git
cd mysql-structure-sync
```

2. 编译应用：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o mysql-struct-sync
```

## 配置说明

1. 复制示例配置文件：

```bash
cp configs/config.example.yaml config.yaml
```

2. 编辑配置文件匹配您的数据库连接：

```yaml
databases:
  source_db:
    dsn: "user:password@tcp(localhost:3306)/source_db?parseTime=true"
  target_db:
    dsn: "user:password@tcp(localhost:3306)/target_db?parseTime=true"

options:
  # 是否合并输出所有表的差异到单个文件
  merge_output: false  # 设为 true 启用合并输出模式
```



## 使用说明

### 表结构比对

比对配置文件中定义的两个数据库的表结构：

```bash
./mysql-struct-sync compare --source source_db --target target_db  # 表名(可选,不选默认全部--table users)
```

该命令将：
1. 连接两个数据库
2. 提取并比对指定表的结构
3. 生成JSON报告文件（如`users_20250702_123045.json`）
4. 生成SQL脚本文件（如`users_20250702_123045.sql`）

### 输出模式

工具支持两种输出模式，可通过配置文件中的 `options.merge_output` 选项控制：

#### 独立输出模式（默认）

每个表生成独立的 JSON 和 SQL 文件：

```bash
# 配置文件设置
options:
  merge_output: false
```

**输出文件示例：**
- `users_20250702_123045.json` - 用户表的差异报告
- `users_20250702_123045.sql` - 用户表的同步脚本
- `orders_20250702_123045.json` - 订单表的差异报告
- `orders_20250702_123045.sql` - 订单表的同步脚本

#### 合并输出模式

所有表的差异合并到单个 JSON 和 SQL 文件中：

```bash
# 配置文件设置
options:
  merge_output: true
```

**输出文件示例：**
- `merged_sync_20250702_123045.json` - 所有表的合并差异报告
- `merged_sync_20250702_123045.sql` - 所有表的合并同步脚本

### 输出文件说明

#### JSON报告（独立模式）

包含单个表的结构差异：

```json
{
  "table": "users",
  "diff": [
    {
      "type": "ADD_COLUMN",
      "name": "nickname",
      "description": "添加列'nickname'",
      "sql": "ALTER TABLE `users` ADD COLUMN `nickname` VARCHAR(255)"
    },
    {
      "type": "MODIFY_COLUMN",
      "name": "email",
      "description": "修改列'email'",
      "sql": "ALTER TABLE `users` MODIFY COLUMN `email` VARCHAR(512) NOT NULL"
    }
  ],
  "status": "待处理"
}
```

#### JSON报告（合并模式）

包含所有表的结构差异：

```json
{
  "tables": [
    {
      "table": "users",
      "diff": [
        {
          "type": "ADD_COLUMN",
          "name": "nickname",
          "description": "添加列'nickname'",
          "sql": "ALTER TABLE `users` ADD COLUMN `nickname` VARCHAR(255)"
        }
      ],
      "status": "待处理"
    },
    {
      "table": "orders",
      "diff": [
        {
          "type": "MODIFY_COLUMN",
          "name": "amount",
          "description": "修改列'amount'",
          "sql": "ALTER TABLE `orders` MODIFY COLUMN `amount` DECIMAL(10,2) NOT NULL"
        }
      ],
      "status": "待处理"
    }
  ],
  "total_tables": 2,
  "total_differences": 2,
  "status": "completed"
}
```

#### SQL脚本（独立模式）

包含单个表的同步SQL语句：

```sql
-- MySQL表结构同步脚本（表名：users）
-- 生成时间：2025-07-02 12:30:45

-- 发现2处差异

START TRANSACTION;

-- 1. ADD_COLUMN: 添加列'nickname'
ALTER TABLE `users` ADD COLUMN `nickname` VARCHAR(255);

-- 2. MODIFY_COLUMN: 修改列'email'
ALTER TABLE `users` MODIFY COLUMN `email` VARCHAR(512) NOT NULL;

COMMIT;
```

