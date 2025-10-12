package sync

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// DifferenceType represents the type of difference between tables
type DifferenceType string

const (
	AddColumn      DifferenceType = "ADD_COLUMN"
	ModifyColumn   DifferenceType = "MODIFY_COLUMN"
	DropColumn     DifferenceType = "DROP_COLUMN"
	AddIndex       DifferenceType = "ADD_INDEX"
	ModifyIndex    DifferenceType = "MODIFY_INDEX"
	DropIndex      DifferenceType = "DROP_INDEX"
	AddPrimaryKey  DifferenceType = "ADD_PRIMARY_KEY"
	DropPrimaryKey DifferenceType = "DROP_PRIMARY_KEY"
)

// Difference represents a single structural difference between tables
type Difference struct {
	Type        DifferenceType `json:"type"`
	Name        string         `json:"name"`        // Column or index name
	Description string         `json:"description"` // Human-readable description
	SQL         string         `json:"sql"`         // SQL to fix the difference
}

// SyncPlan represents the complete plan for synchronizing table structures
type SyncPlan struct {
	TableName   string       `json:"table"`
	Differences []Difference `json:"diff"`
	Status      string       `json:"status"`
}

// MergedSyncPlan represents a collection of sync plans for multiple tables
type MergedSyncPlan struct {
	Tables      []SyncPlan `json:"tables"`
	TotalTables int        `json:"total_tables"`
	TotalDiffs  int        `json:"total_differences"`
	Status      string     `json:"status"`
}

// SaveJSON writes the sync plan to a JSON file
func (p *SyncPlan) SaveJSON(filename string) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal sync plan: %w", err)
	}

	return os.WriteFile(filename, data, 0644)
}

// SaveSQL writes all SQL statements to a SQL file
func (p *SyncPlan) SaveSQL(filename string) error {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("-- MySQL Table Structure Synchronization for '%s'\n", p.TableName))
	builder.WriteString(fmt.Sprintf("-- Generated: %s\n\n", "CURRENT_TIMESTAMP"))

	if len(p.Differences) == 0 {
		builder.WriteString("-- No differences found, structures are identical\n")
	} else {
		builder.WriteString(fmt.Sprintf("-- Found %d differences\n\n", len(p.Differences)))

		// Start transaction
		builder.WriteString("START TRANSACTION;\n\n")

		// Add all SQL statements
		for i, diff := range p.Differences {
			builder.WriteString(fmt.Sprintf("-- %d. %s: %s\n", i+1, diff.Type, diff.Description))
			builder.WriteString(diff.SQL)
			builder.WriteString(";\n\n")
		}

		// Commit
		builder.WriteString("COMMIT;\n")
	}

	return os.WriteFile(filename, []byte(builder.String()), 0644)
}

// SaveJSON writes the merged sync plan to a JSON file
func (m *MergedSyncPlan) SaveJSON(filename string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal merged sync plan: %w", err)
	}

	return os.WriteFile(filename, data, 0644)
}

// SaveSQL writes all SQL statements from all tables to a single SQL file
func (m *MergedSyncPlan) SaveSQL(filename string) error {
	var builder strings.Builder

	builder.WriteString("-- MySQL Table Structure Synchronization (Merged)\n")
	builder.WriteString(fmt.Sprintf("-- Generated: %s\n", "CURRENT_TIMESTAMP"))
	builder.WriteString(fmt.Sprintf("-- Total Tables: %d\n", m.TotalTables))
	builder.WriteString(fmt.Sprintf("-- Total Differences: %d\n\n", m.TotalDiffs))

	if m.TotalDiffs == 0 {
		builder.WriteString("-- No differences found in any table, all structures are identical\n")
		return os.WriteFile(filename, []byte(builder.String()), 0644)
	}

	// Start transaction
	builder.WriteString("START TRANSACTION;\n\n")

	// Process each table
	for tableIndex, table := range m.Tables {
		if len(table.Differences) == 0 {
			continue // Skip tables with no differences
		}

		builder.WriteString(fmt.Sprintf("-- Table: %s (%d differences)\n", table.TableName, len(table.Differences)))
		builder.WriteString(fmt.Sprintf("-- ==========================================\n\n"))

		// Add all SQL statements for this table
		for i, diff := range table.Differences {
			builder.WriteString(fmt.Sprintf("-- %d.%d. %s: %s\n", tableIndex+1, i+1, diff.Type, diff.Description))
			builder.WriteString(diff.SQL)
			builder.WriteString(";\n\n")
		}

		builder.WriteString("\n")
	}

	// Commit
	builder.WriteString("COMMIT;\n")

	return os.WriteFile(filename, []byte(builder.String()), 0644)
}
