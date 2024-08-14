package sdk

import (
	"context"
	"fmt"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
)

func (c *Client) GetDBTables(ctx context.Context, database string) ([]models.CHTable, error) {
	query := fmt.Sprintf("SELECT database, name FROM system.tables where database = '%s'", database)
	rows, err := c.Conn.Query(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("reading tables from Clickhouse: %v", err)
	}

	var tables []models.CHTable
	for rows.Next() {
		var table models.CHTable
		err := rows.ScanStruct(&table)
		if err != nil {
			return nil, fmt.Errorf("scanning Clickhouse table row: %v", err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}
