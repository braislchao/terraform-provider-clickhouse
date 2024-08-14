package sdk

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/common"
	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
)

func (c *Client) GetView(ctx context.Context, database string, view string) (*models.CHView, error) {
	query := fmt.Sprintf("SELECT database, name, engine, as_select, comment FROM system.tables where database = '%s' and name = '%s'", database, view)
	row := c.Conn.QueryRow(ctx, query)

	if row.Err() != nil {
		return nil, fmt.Errorf("reading view from Clickhouse: %v", row.Err())
	}

	var chView models.CHView
	err := row.ScanStruct(&chView)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scanning Clickhouse view row: %v", err)
	}
	// the value read from system.tables is not formatted
	normalizedQuery := common.NormalizeQuery(chView.Query)
	chView.Query = normalizedQuery

	return &chView, nil
}

func (c *Client) CreateView(ctx context.Context, resource models.ViewResource) error {
	query := buildCreateOnClusterSentence(resource)
	err := c.Conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("creating Clickhouse view: %v", err)
	}
	return nil
}

func (c *Client) DeleteView(ctx context.Context, resource models.ViewResource) error {
	query := fmt.Sprintf("DROP VIEW if exists %s.%s %s", resource.Database, resource.Name, common.GetClusterStatement(resource.Cluster))
	err := c.Conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("deleting Clickhouse view: %v", err)
	}
	return nil
}
