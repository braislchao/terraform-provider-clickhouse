package resourcetable

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Triple-Whale/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
)

type CHTable struct {
	Database   string     `ch:"database"`
	Name       string     `ch:"name"`
	EngineFull string     `ch:"engine_full"`
	Engine     string     `ch:"engine"`
	Comment    string     `ch:"comment"`
	Columns    []CHColumn `ch:"columns"`
}

type CHColumn struct {
	Database string `ch:"database"`
	Table    string `ch:"table"`
	Name     string `ch:"name"`
	Type     string `ch:"type"`
	Comment  string `ch:"comment"`
}

type TableResource struct {
	Database     string
	Name         string
	EngineFull   string
	Engine       string
	Cluster      string
	Comment      string
	EngineParams []string
	OrderBy      []string
	Columns      []interface{}
	PartitionBy  []PartitionByResource
	Settings     map[string]string
}

type ColumnResource struct {
	Name     string
	Type     string
	Nullable bool
	Comment  string
}

type PartitionByResource struct {
	By                string
	PartitionFunction string
}

func (t *CHTable) ColumnsToResource() []interface{} {
	var columnResources []interface{}
	for _, column := range t.Columns {
		columnResource := map[string]interface{}{
			"name":     column.Name,
			"type":     removeNullable(column.Type),
			"nullable": isNullable(column.Type),
			"comment":  column.Comment,
		}
		columnResources = append(columnResources, columnResource)
	}

	return columnResources
}

func removeNullable(columnType string) string {
	if strings.HasPrefix(columnType, "Nullable(") && strings.HasSuffix(columnType, ")") {
		return strings.TrimSuffix(strings.TrimPrefix(columnType, "Nullable("), ")")
	}
	return columnType
}

func isNullable(columnType string) bool {
	return strings.HasPrefix(columnType, "Nullable")
}

func (t *CHTable) ToResource() (*TableResource, error) {
	tableResource := TableResource{
		Database:   t.Database,
		Name:       t.Name,
		EngineFull: t.EngineFull,
		Engine:     t.Engine,
		Columns:    t.ColumnsToResource(),
	}

	r := regexp.MustCompile(`\(([^)]*)\)`)

	match := r.FindStringSubmatch(t.EngineFull)
	var engineParams []string
	if len(match) > 1 {
		values := strings.Split(match[1], ",")
		for _, value := range values {
			value = strings.TrimSpace(value)
			engineParams = append(engineParams, strings.TrimSpace(value))
		}
	}

	comment, cluster, _, err := common.UnmarshalComment(t.Comment)
	if err != nil {
		return nil, err
	}

	tableResource.Cluster = cluster
	tableResource.Comment = comment
	tableResource.EngineParams = engineParams

	return &tableResource, nil
}

func (t *TableResource) GetColumnsResourceList() []ColumnResource {
	var columnResources []ColumnResource
	for _, column := range t.Columns {
		columnResources = append(columnResources, ColumnResource{
			Name:     column.(map[string]interface{})["name"].(string),
			Type:     column.(map[string]interface{})["type"].(string),
			Nullable: column.(map[string]interface{})["nullable"].(bool),
			Comment:  column.(map[string]interface{})["comment"].(string),
		})
	}
	return columnResources
}

func (t *TableResource) SetPartitionBy(partitionBy []interface{}) {
	for _, partitionBy := range partitionBy {
		partitionByResource := PartitionByResource{
			By:                partitionBy.(map[string]interface{})["by"].(string),
			PartitionFunction: partitionBy.(map[string]interface{})["partition_function"].(string),
		}
		t.PartitionBy = append(t.PartitionBy, partitionByResource)
	}
}

func (t *TableResource) HasColumn(columnName string) bool {
	for _, column := range t.GetColumnsResourceList() {
		if column.Name == columnName {
			return true
		}
	}
	return false
}

func (t *TableResource) Validate(diags diag.Diagnostics) {
	t.validateOrderBy(diags)
	t.validatePartitionBy(diags)
}

func (t *TableResource) validateOrderBy(diags diag.Diagnostics) {
	for _, orderField := range t.OrderBy {
		if t.HasColumn(orderField) == false {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "wrong value",
				Detail:   fmt.Sprintf("order by field '%s' is not a column", orderField),
			})
		}
	}
}

func (t *TableResource) validatePartitionBy(diags diag.Diagnostics) {
	for _, partitionBy := range t.PartitionBy {
		if t.HasColumn(partitionBy.By) == false {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "wrong value",
				Detail:   fmt.Sprintf("partition by field '%s' is not a column", partitionBy.By),
			})
		}
	}
}
