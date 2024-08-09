package resourcetable

import (
	"context"
	"fmt"
	"strings"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type CHTableService common.ApiClient

func copyToMap(iface interface{}) map[string]interface{} {
	mapCopy := iface.(map[string]interface{})
	mapNew := make(map[string]interface{})
	for k, v := range mapCopy {
		mapNew[k] = v
	}
	return mapNew
}

func (ts *CHTableService) UpdateTable(ctx context.Context, table TableResource, resourceData *schema.ResourceData) error {
	clusterStatement := common.GetClusterStatement(table.Cluster)

	if resourceData.HasChange("comment") {
		err := executeQuery(ctx, ts, fmt.Sprintf("ALTER TABLE %s.%s %s MODIFY COMMENT '%s'", table.Database, table.Name, clusterStatement, table.Comment))
		if err != nil {
			return err
		}
	}

	if resourceData.HasChange("column") {
		old, new := resourceData.GetChange("column")
		oldColumns := old.([]interface{})
		newColumns := new.([]interface{})

		oldColumnsMap := createColumnsMap(oldColumns)
		newColumnsMap := createColumnsMap(newColumns)

		location := "FIRST"
		for _, column := range newColumns {
			columnMap := copyToMap(column)
			columnMap["location"] = location

			err := handleColumnChanges(ctx, ts, table, clusterStatement, columnMap, oldColumnsMap)
			if err != nil {
				return err
			}

			location = "AFTER " + columnMap["name"].(string)
		}

		err := dropOldColumns(ctx, ts, table, clusterStatement, oldColumns, newColumnsMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func executeQuery(ctx context.Context, ts *CHTableService, query string) error {
	if common.DebugEnabled {
		formattedQuery, err := formatQuery(ctx, ts, query)
		if err != nil {
			return err
		}
		tflog.Debug(ctx, "executing query: \n\n"+formattedQuery+"\n\n")
	}

	// Execute the query
	err := (*ts.ClickhouseConnection).Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("executing query: %v", err)
	}
	return nil
}

func formatQuery(ctx context.Context, ts *CHTableService, query string) (string, error) {
	escapedQuery := strings.ReplaceAll(query, "'", "''")
	formatQueryStmt := fmt.Sprintf("SELECT formatQuery('%s')", escapedQuery)
	row := (*ts.ClickhouseConnection).QueryRow(ctx, formatQueryStmt)

	var formattedQueryResult string
	if err := row.Scan(&formattedQueryResult); err != nil {
		return "", err
	}
	return formattedQueryResult, nil
}

func createColumnsMap(columns []interface{}) map[string]map[string]interface{} {
	columnsMap := make(map[string]map[string]interface{})
	for _, column := range columns {
		columnMap := column.(map[string]interface{})
		columnName := columnMap["name"].(string)
		columnsMap[columnName] = columnMap
	}
	return columnsMap
}

func handleColumnChanges(ctx context.Context, ts *CHTableService, table TableResource, clusterStatement string, columnMap map[string]interface{}, oldColumnsMap map[string]map[string]interface{}) error {
	columnName := columnMap["name"].(string)
	if _, exists := oldColumnsMap[columnName]; !exists {
		return executeQuery(ctx, ts, fmt.Sprintf(
			"ALTER TABLE %s.%s %s ADD COLUMN %s %s %s %s %s %s",
			table.Database, table.Name, clusterStatement, columnMap["name"], columnMap["type"], columnMap["default_kind"],
			columnMap["default_expression"], getComment(columnMap["comment"].(string)), columnMap["location"]))
	} else {
		oldColumn := oldColumnsMap[columnName]
		if oldColumn["type"] != columnMap["type"] {
			return executeQuery(ctx, ts, fmt.Sprintf(
				"ALTER TABLE %s.%s %s MODIFY COLUMN %s %s",
				table.Database, table.Name, clusterStatement, columnMap["name"], columnMap["type"]))
		}
		if oldColumn["comment"] != columnMap["comment"] {
			return executeQuery(ctx, ts, fmt.Sprintf(
				"ALTER TABLE %s.%s %s COMMENT COLUMN %s '%s'",
				table.Database, table.Name, clusterStatement, columnMap["name"], columnMap["comment"]))
		}
		if oldColumn["default_kind"] != columnMap["default_kind"] || oldColumn["default_expression"] != columnMap["default_expression"] {
			return executeQuery(ctx, ts, fmt.Sprintf(
				"ALTER TABLE %s.%s %s MODIFY COLUMN %s %s %s",
				table.Database, table.Name, clusterStatement, columnMap["name"], columnMap["default_kind"], columnMap["default_expression"]))
		}
	}
	return nil
}

func dropOldColumns(ctx context.Context, ts *CHTableService, table TableResource, clusterStatement string, oldColumns []interface{}, newColumnsMap map[string]map[string]interface{}) error {
	for _, column := range oldColumns {
		columnMap := column.(map[string]interface{})
		if _, exists := newColumnsMap[columnMap["name"].(string)]; !exists {
			err := executeQuery(ctx, ts, fmt.Sprintf(
				"ALTER TABLE %s.%s %s DROP COLUMN %s",
				table.Database, table.Name, clusterStatement, columnMap["name"]))
			if err != nil {
				return fmt.Errorf("dropping columns from Clickhouse table: %v", err)
			}
		}
	}
	return nil
}

func (ts *CHTableService) GetDBTables(ctx context.Context, database string) ([]CHTable, error) {
	query := fmt.Sprintf("SELECT database, name FROM system.tables where database = '%s'", database)
	rows, err := (*ts.ClickhouseConnection).Query(ctx, query)

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
	row := (*ts.ClickhouseConnection).QueryRow(ctx, query)

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
	rows, err := (*ts.ClickhouseConnection).Query(ctx, query)

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
	rows, err := (*ts.ClickhouseConnection).Query(ctx, query)

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
	return executeQuery(ctx, ts, query)
}

func (ts *CHTableService) DeleteTable(ctx context.Context, tableResource TableResource) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s %s", tableResource.Database, tableResource.Name, common.GetClusterStatement(tableResource.Cluster))
	return executeQuery(ctx, ts, query)
}
