package sync

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// TableModel represents the structure of a MySQL table
type TableModel struct {
	Name       string
	Columns    []ColumnModel
	Indexes    []IndexModel
	PrimaryKey *PrimaryKeyModel
}

// ColumnModel represents a column in a MySQL table
type ColumnModel struct {
	Name         string
	DataType     string
	IsNullable   string
	ColumnKey    string
	DefaultValue sql.NullString
	Extra        string
	OnUpdate     sql.NullString // 新增
}

// IndexModel represents an index in a MySQL table
type IndexModel struct {
	Name      string
	Columns   []string
	NonUnique int
	IndexType string
}

// PrimaryKeyModel represents a primary key in a MySQL table
type PrimaryKeyModel struct {
	Name    string
	Columns []string
}

// Comparer handles comparison between MySQL tables
type Comparer struct {
	SourceDB *sql.DB
	TargetDB *sql.DB
}

// NewComparer creates a new comparer with database connections
func NewComparer(sourceDSN, targetDSN string) (*Comparer, error) {
	sourceDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source database: %w", err)
	}

	if err = sourceDB.Ping(); err != nil {
		return nil, fmt.Errorf("source database connection failed: %w", err)
	}

	targetDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target database: %w", err)
	}

	if err = targetDB.Ping(); err != nil {
		return nil, fmt.Errorf("target database connection failed: %w", err)
	}

	return &Comparer{
		SourceDB: sourceDB,
		TargetDB: targetDB,
	}, nil
}

// Close closes database connections
func (c *Comparer) Close() {
	if c.SourceDB != nil {
		c.SourceDB.Close()
	}
	if c.TargetDB != nil {
		c.TargetDB.Close()
	}
}

// Compare compares the structure of a table between source and target databases
func (c *Comparer) Compare(tableName string) (*SyncPlan, error) {
	// Get table structures
	sourceTable, err := c.getTableStructure(c.SourceDB, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get source table structure: %w", err)
	}

	targetTable, err := c.getTableStructure(c.TargetDB, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get target table structure: %w", err)
	}

	// Create sync plan
	plan := &SyncPlan{
		TableName:   tableName,
		Differences: []Difference{},
		Status:      "pending",
	}

	// Compare columns
	c.compareColumns(sourceTable, targetTable, plan)

	// Compare indexes
	c.compareIndexes(sourceTable, targetTable, plan)

	// Compare primary keys
	c.comparePrimaryKeys(sourceTable, targetTable, plan)

	return plan, nil
}

// getTableStructure fetches the complete structure of a table
func (c *Comparer) getTableStructure(db *sql.DB, tableName string) (*TableModel, error) {
	table := &TableModel{
		Name:    tableName,
		Columns: []ColumnModel{},
		Indexes: []IndexModel{},
	}

	// Get database name from connection
	var dbName string
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to get database name: %w", err)
	}

	// Get columns
	columnsQuery := `
		SELECT 
			COLUMN_NAME, 
			COLUMN_TYPE, 
			IS_NULLABLE, 
			COLUMN_KEY, 
			COLUMN_DEFAULT,
			EXTRA
		FROM 
			INFORMATION_SCHEMA.COLUMNS
		WHERE 
			TABLE_SCHEMA = ? 
			AND TABLE_NAME = ?
		ORDER BY 
			ORDINAL_POSITION
	`

	rows, err := db.Query(columnsQuery, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnModel
		if err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			&col.ColumnKey,
			&col.DefaultValue,
			&col.Extra,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column row: %w", err)
		}

		// 解析EXTRA字段，提取ON UPDATE CURRENT_TIMESTAMP
		if strings.Contains(col.Extra, "on update CURRENT_TIMESTAMP") {
			col.OnUpdate = sql.NullString{String: "CURRENT_TIMESTAMP", Valid: true}
		} else {
			col.OnUpdate = sql.NullString{Valid: false}
		}

		table.Columns = append(table.Columns, col)

		// Check if this column is part of primary key
		if col.ColumnKey == "PRI" {
			if table.PrimaryKey == nil {
				table.PrimaryKey = &PrimaryKeyModel{
					Name:    "PRIMARY",
					Columns: []string{},
				}
			}
			table.PrimaryKey.Columns = append(table.PrimaryKey.Columns, col.Name)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column rows: %w", err)
	}

	// Get indexes
	indexQuery := `
		SELECT 
			INDEX_NAME,
			COLUMN_NAME,
			NON_UNIQUE,
			INDEX_TYPE
		FROM 
			INFORMATION_SCHEMA.STATISTICS
		WHERE 
			TABLE_SCHEMA = ? 
			AND TABLE_NAME = ?
		ORDER BY 
			INDEX_NAME, 
			SEQ_IN_INDEX
	`

	rows, err = db.Query(indexQuery, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexModel)

	for rows.Next() {
		var idxName, colName, idxType string
		var nonUnique int

		if err := rows.Scan(&idxName, &colName, &nonUnique, &idxType); err != nil {
			return nil, fmt.Errorf("failed to scan index row: %w", err)
		}

		// Skip PRIMARY key as it's handled separately
		if idxName == "PRIMARY" {
			continue
		}

		// Create or update the index in our map
		if idx, ok := indexMap[idxName]; ok {
			idx.Columns = append(idx.Columns, colName)
		} else {
			indexMap[idxName] = &IndexModel{
				Name:      idxName,
				Columns:   []string{colName},
				NonUnique: nonUnique,
				IndexType: idxType,
			}
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	// Convert map to slice
	for _, idx := range indexMap {
		table.Indexes = append(table.Indexes, *idx)
	}

	return table, nil
}

// compareColumns compares columns between source and target tables
func (c *Comparer) compareColumns(source, target *TableModel, plan *SyncPlan) {
	// Map of columns for easy lookup
	sourceColumns := make(map[string]ColumnModel)
	targetColumns := make(map[string]ColumnModel)

	for _, col := range source.Columns {
		sourceColumns[col.Name] = col
	}

	for _, col := range target.Columns {
		targetColumns[col.Name] = col
	}

	// Find columns to add (in source but not in target)
	for name, sourceCol := range sourceColumns {
		if _, exists := targetColumns[name]; !exists {
			// Column needs to be added to target
			diff := Difference{
				Type:        AddColumn,
				Name:        name,
				Description: fmt.Sprintf("Add column '%s'", name),
				SQL:         c.generateAddColumnSQL(source.Name, sourceCol),
			}
			plan.Differences = append(plan.Differences, diff)
		}
	}

	// Find columns to modify (different definitions)
	for name, targetCol := range targetColumns {
		sourceCol, exists := sourceColumns[name]
		if exists {
			// Compare column definitions
			if c.columnsAreDifferent(sourceCol, targetCol) {
				diff := Difference{
					Type:        ModifyColumn,
					Name:        name,
					Description: fmt.Sprintf("Modify column '%s'", name),
					SQL:         c.generateModifyColumnSQL(source.Name, sourceCol),
				}
				plan.Differences = append(plan.Differences, diff)
			}
		} else {
			// Column exists in target but not in source (needs to be dropped)
			diff := Difference{
				Type:        DropColumn,
				Name:        name,
				Description: fmt.Sprintf("Drop column '%s'", name),
				SQL:         fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", source.Name, name),
			}
			plan.Differences = append(plan.Differences, diff)
		}
	}
}

// compareIndexes compares indexes between source and target tables
func (c *Comparer) compareIndexes(source, target *TableModel, plan *SyncPlan) {
	// Map of indexes for easy lookup
	sourceIndexes := make(map[string]IndexModel)
	targetIndexes := make(map[string]IndexModel)

	for _, idx := range source.Indexes {
		sourceIndexes[idx.Name] = idx
	}

	for _, idx := range target.Indexes {
		targetIndexes[idx.Name] = idx
	}

	// Find indexes to add (in source but not in target)
	for name, sourceIdx := range sourceIndexes {
		if _, exists := targetIndexes[name]; !exists {
			// Index needs to be added to target
			diff := Difference{
				Type:        AddIndex,
				Name:        name,
				Description: fmt.Sprintf("Add index '%s'", name),
				SQL:         c.generateAddIndexSQL(source.Name, sourceIdx),
			}
			plan.Differences = append(plan.Differences, diff)
		} else {
			// Index exists in both, check if they're different
			targetIdx := targetIndexes[name]
			if c.indexesAreDifferent(sourceIdx, targetIdx) {
				// Drop and recreate index
				dropDiff := Difference{
					Type:        DropIndex,
					Name:        name,
					Description: fmt.Sprintf("Drop index '%s'", name),
					SQL:         fmt.Sprintf("DROP INDEX `%s` ON `%s`", name, source.Name),
				}
				plan.Differences = append(plan.Differences, dropDiff)

				addDiff := Difference{
					Type:        AddIndex,
					Name:        name,
					Description: fmt.Sprintf("Recreate index '%s'", name),
					SQL:         c.generateAddIndexSQL(source.Name, sourceIdx),
				}
				plan.Differences = append(plan.Differences, addDiff)
			}
		}
	}

	// Find indexes to drop (in target but not in source)
	for name, _ := range targetIndexes {
		if _, exists := sourceIndexes[name]; !exists {
			// Index exists in target but not in source
			diff := Difference{
				Type:        DropIndex,
				Name:        name,
				Description: fmt.Sprintf("Drop index '%s'", name),
				SQL:         fmt.Sprintf("DROP INDEX `%s` ON `%s`", name, source.Name),
			}
			plan.Differences = append(plan.Differences, diff)
		}
	}
}

// comparePrimaryKeys compares primary keys between source and target tables
func (c *Comparer) comparePrimaryKeys(source, target *TableModel, plan *SyncPlan) {
	if source.PrimaryKey == nil && target.PrimaryKey == nil {
		// No primary key in either table
		return
	}

	if source.PrimaryKey == nil && target.PrimaryKey != nil {
		// Target has primary key but source doesn't - drop primary key
		diff := Difference{
			Type:        DropPrimaryKey,
			Name:        "PRIMARY",
			Description: "Drop primary key",
			SQL:         fmt.Sprintf("ALTER TABLE `%s` DROP PRIMARY KEY", source.Name),
		}
		plan.Differences = append(plan.Differences, diff)
		return
	}

	if source.PrimaryKey != nil && target.PrimaryKey == nil {
		// Source has primary key but target doesn't - add primary key
		diff := Difference{
			Type:        AddPrimaryKey,
			Name:        "PRIMARY",
			Description: "Add primary key",
			SQL:         c.generateAddPrimaryKeySQL(source.Name, *source.PrimaryKey),
		}
		plan.Differences = append(plan.Differences, diff)
		return
	}

	// Both have primary keys - check if they're different
	if c.primaryKeysAreDifferent(*source.PrimaryKey, *target.PrimaryKey) {
		// Drop and recreate primary key
		dropDiff := Difference{
			Type:        DropPrimaryKey,
			Name:        "PRIMARY",
			Description: "Drop existing primary key",
			SQL:         fmt.Sprintf("ALTER TABLE `%s` DROP PRIMARY KEY", source.Name),
		}
		plan.Differences = append(plan.Differences, dropDiff)

		addDiff := Difference{
			Type:        AddPrimaryKey,
			Name:        "PRIMARY",
			Description: "Add new primary key",
			SQL:         c.generateAddPrimaryKeySQL(source.Name, *source.PrimaryKey),
		}
		plan.Differences = append(plan.Differences, addDiff)
	}
}

// columnsAreDifferent checks if two columns have different definitions
func (c *Comparer) columnsAreDifferent(source, target ColumnModel) bool {
	// Compare basic attributes
	if source.DataType != target.DataType ||
		source.IsNullable != target.IsNullable ||
		source.Extra != target.Extra {
		return true
	}
	// Compare default values (handle NULL case)
	if source.DefaultValue.Valid != target.DefaultValue.Valid {
		return true
	}
	if source.DefaultValue.Valid && target.DefaultValue.Valid &&
		source.DefaultValue.String != target.DefaultValue.String {
		return true
	}
	// 新增：比较OnUpdate属性
	if source.OnUpdate.Valid != target.OnUpdate.Valid {
		return true
	}
	if source.OnUpdate.Valid && target.OnUpdate.Valid &&
		source.OnUpdate.String != target.OnUpdate.String {
		return true
	}
	return false
}

// indexesAreDifferent checks if two indexes have different definitions
func (c *Comparer) indexesAreDifferent(source, target IndexModel) bool {
	if source.NonUnique != target.NonUnique || source.IndexType != target.IndexType {
		return true
	}

	// Check if columns are the same (order matters for indexes)
	if len(source.Columns) != len(target.Columns) {
		return true
	}

	for i, col := range source.Columns {
		if col != target.Columns[i] {
			return true
		}
	}

	return false
}

// primaryKeysAreDifferent checks if two primary keys have different definitions
func (c *Comparer) primaryKeysAreDifferent(source, target PrimaryKeyModel) bool {
	// Check if columns are the same (order matters for primary keys)
	if len(source.Columns) != len(target.Columns) {
		return true
	}

	for i, col := range source.Columns {
		if col != target.Columns[i] {
			return true
		}
	}

	return false
}

// generateAddColumnSQL generates SQL to add a column
func (c *Comparer) generateAddColumnSQL(tableName string, column ColumnModel) string {
	var parts []string

	parts = append(parts, fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s",
		tableName, column.Name, column.DataType))

	// NOT NULL constraint
	if column.IsNullable == "NO" {
		parts = append(parts, "NOT NULL")
	}

	// Default value
	if column.DefaultValue.Valid {
		if column.DefaultValue.String == "CURRENT_TIMESTAMP" {
			parts = append(parts, "DEFAULT CURRENT_TIMESTAMP")
		} else {
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", column.DefaultValue.String))
		}
	}

	// ON UPDATE CURRENT_TIMESTAMP
	if column.OnUpdate.Valid && column.OnUpdate.String == "CURRENT_TIMESTAMP" {
		parts = append(parts, "ON UPDATE CURRENT_TIMESTAMP")
	}

	// Auto increment
	if strings.Contains(column.Extra, "auto_increment") {
		parts = append(parts, "AUTO_INCREMENT")
	}

	return strings.Join(parts, " ")
}

// generateModifyColumnSQL generates SQL to modify a column
func (c *Comparer) generateModifyColumnSQL(tableName string, column ColumnModel) string {
	var parts []string

	parts = append(parts, fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` %s",
		tableName, column.Name, column.DataType))

	// NOT NULL constraint
	if column.IsNullable == "NO" {
		parts = append(parts, "NOT NULL")
	}

	// Default value
	if column.DefaultValue.Valid {
		if column.DefaultValue.String == "CURRENT_TIMESTAMP" {
			parts = append(parts, "DEFAULT CURRENT_TIMESTAMP")
		} else {
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", column.DefaultValue.String))
		}
	}

	// ON UPDATE CURRENT_TIMESTAMP
	if column.OnUpdate.Valid && column.OnUpdate.String == "CURRENT_TIMESTAMP" {
		parts = append(parts, "ON UPDATE CURRENT_TIMESTAMP")
	}

	// Auto increment
	if strings.Contains(column.Extra, "auto_increment") {
		parts = append(parts, "AUTO_INCREMENT")
	}

	return strings.Join(parts, " ")
}

// generateAddIndexSQL generates SQL to add an index
func (c *Comparer) generateAddIndexSQL(tableName string, index IndexModel) string {
	var indexType string
	if index.NonUnique == 0 {
		indexType = "UNIQUE"
	} else {
		indexType = "INDEX"
	}

	columns := make([]string, len(index.Columns))
	for i, col := range index.Columns {
		columns[i] = fmt.Sprintf("`%s`", col)
	}

	return fmt.Sprintf("CREATE %s INDEX `%s` ON `%s` (%s) USING %s",
		indexType, index.Name, tableName, strings.Join(columns, ", "), index.IndexType)
}

// generateAddPrimaryKeySQL generates SQL to add a primary key
func (c *Comparer) generateAddPrimaryKeySQL(tableName string, pk PrimaryKeyModel) string {
	columns := make([]string, len(pk.Columns))
	for i, col := range pk.Columns {
		columns[i] = fmt.Sprintf("`%s`", col)
	}

	return fmt.Sprintf("ALTER TABLE `%s` ADD PRIMARY KEY (%s)",
		tableName, strings.Join(columns, ", "))
}

// GetAllTableNames 获取指定数据库的所有表名
func (c *Comparer) GetAllTableNames(db *sql.DB) ([]string, error) {
	var dbName string
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to get database name: %w", err)
	}

	rows, err := db.Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?", dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table names: %w", err)
	}
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table names: %w", err)
	}
	return tableNames, nil
}

// GetCreateTableSQL 获取指定表的建表SQL
func (c *Comparer) GetCreateTableSQL(db *sql.DB, tableName string) (string, error) {
	row := db.QueryRow("SHOW CREATE TABLE `" + tableName + "`")
	var tbl, createSQL string
	if err := row.Scan(&tbl, &createSQL); err != nil {
		return "", err
	}
	return createSQL, nil
}

// CreateTable 在指定数据库执行建表SQL
func (c *Comparer) CreateTable(db *sql.DB, createSQL string) error {
	_, err := db.Exec(createSQL)
	return err
}

// DropTable 删除指定表
func (c *Comparer) DropTable(db *sql.DB, tableName string) error {
	_, err := db.Exec("DROP TABLE IF EXISTS `" + tableName + "`")
	return err
}
