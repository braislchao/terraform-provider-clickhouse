package resourcetable

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Triple-Whale/terraform-provider-clickhouse/pkg/common"
)

type CHTableService struct {
	CHConnection *driver.Conn
}

func (ts *CHTableService) GetDBTables(ctx context.Context, database string) ([]CHTable, error) {
	query := fmt.Sprintf("SELECT database, name FROM system.tables where database = '%s'", database)
	rows, err := (*ts.CHConnection).Query(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("reading tables from Clickhouse: %v", err)
	}

	var tables []CHTable
	for rows.Next() {
		var table CHTable
		err := rows.ScanStruct(&table)
		if err != nil {
			return nil, fmt.Errorf("scanning Clickhouse table row: %v", err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func (ts *CHTableService) GetTable(ctx context.Context, database string, table string) (*CHTable, error) {
	query := fmt.Sprintf("SELECT database, name, engine_full, engine, comment FROM system.tables where database = '%s' and name = '%s'", database, table)
	row := (*ts.CHConnection).QueryRow(ctx, query)

	if row.Err() != nil {
		return nil, fmt.Errorf("reading table from Clickhouse: %v", row.Err())
	}

	var chTable CHTable
	err := row.ScanStruct(&chTable)
	if err != nil && strings.Contains(err.Error(), "no rows in result set") {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scanning Clickhouse table row: %v", err)
	}

	chTable.Columns, err = ts.getTableColumns(ctx, database, table)
	if err != nil {
		return nil, fmt.Errorf("getting columns for Clickhouse table: %v", err)
	}

	chTable.Indexes, err = ts.getTableIndexes(ctx, database, table)
	if err != nil {
		return nil, fmt.Errorf("getting indexes for Clickhouse table: %v", err)
	}

	return &chTable, nil
}

func (ts *CHTableService) getTableIndexes(ctx context.Context, database string, table string) ([]CHIndex, error) {
	query := fmt.Sprintf(
		"SELECT name, expr, type, granularity FROM system.data_skipping_indices WHERE database = '%s' AND table = '%s'",
		database,
		table,
	)
	rows, err := (*ts.CHConnection).Query(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("reading indexes from Clickhouse: %v", err)
	}

	var chIndexes []CHIndex
	for rows.Next() {
		var index CHIndex
		err := rows.ScanStruct(&index)
		if err != nil {
			return nil, fmt.Errorf("scanning Clickhouse index row: %v", err)
		}
		chIndexes = append(chIndexes, index)
	}
	return chIndexes, nil
}

func (ts *CHTableService) getTableColumns(ctx context.Context, database string, table string) ([]CHColumn, error) {
	query := fmt.Sprintf(
		"SELECT database, table, name, type, comment, default_kind, default_expression FROM system.columns WHERE database = '%s' AND table = '%s'",
		database,
		table,
	)
	rows, err := (*ts.CHConnection).Query(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("reading columns from Clickhouse: %v", err)
	}

	var chColumns []CHColumn
	for rows.Next() {
		var column CHColumn
		err := rows.ScanStruct(&column)
		if err != nil {
			return nil, fmt.Errorf("scanning Clickhouse column row: %v", err)
		}
		chColumns = append(chColumns, column)
	}
	return chColumns, nil
}

func (ts *CHTableService) CreateTable(ctx context.Context, tableResource TableResource) error {
	query := buildCreateOnClusterSentence(tableResource)
	err := (*ts.CHConnection).Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("creating Clickhouse table: %v", err)
	}
	return nil
}

func (ts *CHTableService) DeleteTable(ctx context.Context, tableResource TableResource) error {
	query := fmt.Sprintf("DROP TABLE %s.%s %s", tableResource.Database, tableResource.Name, common.GetClusterStatement(tableResource.Cluster))
	err := (*ts.CHConnection).Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("deleting Clickhouse table: %v", err)
	}
	return nil
}
