package sdk

import (
	"context"
	"fmt"
	"strings"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
)

func UpdateTTL(ctx context.Context, c *Client, table models.TableResource, clusterStatement string, newTTL map[string]interface{}) error {
	var ttlExprs []string
	for k, v := range newTTL {
		ttlExpr := fmt.Sprintf("%s %s", k, v)
		ttlExprs = append(ttlExprs, ttlExpr)
	}

	ttlExprsStatement := strings.Join(ttlExprs, ", ")
	if ttlExprsStatement != "" {
		modifyTTLQuery := fmt.Sprintf("ALTER TABLE %s.%s %s MODIFY TTL %s",
			table.Database, table.Name, clusterStatement, ttlExprsStatement)
		err := executeQuery(ctx, c, modifyTTLQuery)
		if err != nil {
			return err
		}
	}

	if ttlExprsStatement == "" {
		removeTTLQuery := fmt.Sprintf("ALTER TABLE %s.%s %s (REMOVE TTL)",
			table.Database, table.Name, clusterStatement)
		err := executeQuery(ctx, c, removeTTLQuery)
		if err != nil {
			return err
		}
	}

	return nil
}
