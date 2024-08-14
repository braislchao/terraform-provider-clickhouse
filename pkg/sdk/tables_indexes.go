package sdk

import (
	"context"
	"fmt"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
)

func (c *Client) GetIndexDefintions(indexes []models.IndexDefinition) []map[string]interface{} {
	var ret []map[string]interface{}

	for _, index := range indexes {
		ret = append(ret, map[string]interface{}{
			"name":        index.Name,
			"expression":  index.Expression,
			"type":        index.Type,
			"granularity": index.Granularity,
		})
	}
	return ret
}

func (c *Client) getIndexes(ctx context.Context, database string, table string) ([]models.CHIndex, error) {
	query := fmt.Sprintf(
		"SELECT name, expr, type, granularity FROM system.data_skipping_indices WHERE database = '%s' AND table = '%s'",
		database,
		table,
	)
	rows, err := c.Conn.Query(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("reading indexes from Clickhouse: %v", err)
	}

	var chIndexes []models.CHIndex
	for rows.Next() {
		var index models.CHIndex
		err := rows.ScanStruct(&index)
		if err != nil {
			return nil, fmt.Errorf("scanning Clickhouse index row: %v", err)
		}
		chIndexes = append(chIndexes, index)
	}
	return chIndexes, nil
}
