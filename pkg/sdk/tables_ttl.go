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

	// since Clickhouse does not support altering individual
	// TTL statements, they need to be removed and then reapplied
	if hasTTL {
		removeTTLQuery := fmt.Sprintf("ALTER TABLE %s.%s %s REMOVE TTL",
			table.Database, table.Name, clusterStatement)
		err = executeQuery(ctx, c, removeTTLQuery)
		if err != nil {
			return err
		}
	}

	// Re-apply TTL if it was removed
	if !hasTTL {
		var ttlExprs []string
		for k, v := range newTTL {
			ttlExpr := fmt.Sprintf("%s %s", k, v)
			ttlExprs = append(ttlExprs, ttlExpr)
		}

		ttlExprsStatement := strings.Join(ttlExprs, ", ")
		if ttlExprsStatement != "" {
			modifyTTLQuery := fmt.Sprintf("ALTER TABLE %s.%s %s MODIFY TTL %s",
				table.Database, table.Name, clusterStatement, ttlExprsStatement)
			err = executeQuery(ctx, c, modifyTTLQuery)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func tableHasTTL(ctx context.Context, c *Client, table models.TableResource) (bool, error) {
	query := fmt.Sprintf(
		"SELECT create_table_query FROM system.tables where database = '%s' and table = '%s'",
		table.Database, table.Name,
	)
	var createTableQuery string
	row := c.Conn.QueryRow(ctx, query)

	if err := row.Scan(&createTableQuery); err != nil {
		return false, err
	}

	return strings.Contains(createTableQuery, "TTL"), nil
}
