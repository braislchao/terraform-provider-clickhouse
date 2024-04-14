package resourcedb

import resourcetable "github.com/Triple-Whale/terraform-provider-clickhouse/v4/pkg/resources/table"

type CHDBResources struct {
	CHTables []resourcetable.CHTable
}
