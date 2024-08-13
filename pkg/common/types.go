package common

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Client struct {
	Connection *driver.Conn
}
