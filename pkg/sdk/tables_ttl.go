package sdk

import (
	"context"
	"fmt"
	"strings"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
)

func UpdateTTL(ctx context.Context, c *Client, table models.TableResource, clusterStatement string, newTTL map[string]interface{}) error {
	hasTTL, err := tableHasTTL(ctx, c, table)
	if err != nil {
		return err
	}

	if hasTTL {
		removeTTLQuery := fmt.Sprintf("ALTER TABLE %s.%s %s REMOVE TTL",
			table.Database, table.Name, clusterStatement)
		err = executeQuery(ctx, c, removeTTLQuery)
		if err != nil {
			return err
		}
	}

	for k, v := range newTTL {
		query := fmt.Sprintf("ALTER TABLE %s.%s %s MODIFY TTL %s %s",
			table.Database, table.Name, clusterStatement, k, v)
		err := executeQuery(ctx, c, query)
		if err != nil {
			return err
		}
	}

	return nil
}

func tableHasTTL(ctx context.Context, c *Client, table models.TableResource) (bool, error) {
	query := fmt.Sprintf(
		"SELECT create_table_query FROM system.tables where database = '%s' and table = '%s'", table.Database, table.Name,
	)
	row := c.Conn.QueryRow(ctx, query)
	var (
		create_table_query string
	)

	if err := row.Scan(&create_table_query); err != nil {
		return false, err
	}

	return strings.Contains(create_table_query, "TTL"), nil
}
