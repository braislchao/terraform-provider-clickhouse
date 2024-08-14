package sdk

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Client struct {
	Connection *driver.Conn
}

func (client *Client) FormatQuery(ctx context.Context, query string) (string, error) {
	conn := client.Connection
	formatQueryStmt := `SELECT formatQuery($1)`
	row := (*conn).QueryRow(ctx, formatQueryStmt, query)

	var formattedQueryResult string
	if err := row.Scan(&formattedQueryResult); err != nil {
		return "", err
	}
	return formattedQueryResult, nil
}
