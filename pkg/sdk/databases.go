package sdk

import (
	"context"
	"fmt"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
)

func (client *Client) GetDBResources(ctx context.Context, database string) (*models.CHDBResources, error) {
	var dbResources models.CHDBResources
	var err error

	dbResources.CHTables, err = client.GetDBTables(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("error getting tables from database: %v", err)
	}

	return &dbResources, nil
}
