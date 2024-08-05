package resourcedb

import resourcetable "github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/resources/table"

type CHDBResources struct {
	CHTables []resourcetable.CHTable
}
