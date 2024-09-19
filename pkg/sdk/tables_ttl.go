package sdk

import (
	"context"
	"fmt"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
)

func UpdateTTL(ctx context.Context, c *Client, table models.TableResource, clusterStatement string, newTTL map[string]interface{}) error {
	removeTTLQuery := fmt.Sprintf("ALTER TABLE %s.%s %s REMOVE TTL",
		table.Database, table.Name, clusterStatement)
	err := executeQuery(ctx, c, removeTTLQuery)
	if err != nil {
		return err
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
