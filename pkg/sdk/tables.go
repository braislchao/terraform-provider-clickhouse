package sdk

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/common"
	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func (c *Client) UpdateTable(ctx context.Context, table models.TableResource, resourceData *schema.ResourceData) error {
	clusterStatement := common.GetClusterStatement(table.Cluster)

	if resourceData.HasChange("comment") {
		query := fmt.Sprintf("ALTER TABLE %s.%s %s MODIFY COMMENT '%s'", table.Database, table.Name, clusterStatement, table.Comment)
		err := executeQuery(ctx, c, query)
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

			columnName := columnMap["name"].(string)

			err := UpdateColumns(ctx, c, table, clusterStatement, columnMap, oldColumnsMap)
			if err != nil {
				return err
			}

			location = "AFTER " + columnName
		}

		err := dropOldColumns(ctx, c, table, clusterStatement, oldColumns, newColumnsMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) GetTable(ctx context.Context, database string, table string) (*models.CHTable, error) {
	query := fmt.Sprintf("SELECT database, name, engine_full, engine, comment FROM system.tables where database = '%s' and name = '%s'", database, table)
	row := c.Conn.QueryRow(ctx, query)

	if row.Err() != nil {
		return nil, fmt.Errorf("reading table from Clickhouse: %v", row.Err())
	}

	var chTable models.CHTable
	err := row.ScanStruct(&chTable)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scanning Clickhouse table row: %v", err)
	}

	chTable.Columns, err = c.getColumns(ctx, database, table)
	if err != nil {
		return nil, fmt.Errorf("getting columns for Clickhouse table: %v", err)
	}

	chTable.Indexes, err = c.getIndexes(ctx, database, table)
	if err != nil {
		return nil, fmt.Errorf("getting indexes for Clickhouse table: %v", err)
	}

	return &chTable, nil
}

func (c *Client) CreateTable(ctx context.Context, tableResource models.TableResource) error {
	query := buildCreateTableOnClusterSentence(tableResource)
	return executeQuery(ctx, c, query)
}

func (c *Client) DeleteTable(ctx context.Context, tableResource models.TableResource) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s %s", tableResource.Database, tableResource.Name, common.GetClusterStatement(tableResource.Cluster))
	return executeQuery(ctx, c, query)
}
