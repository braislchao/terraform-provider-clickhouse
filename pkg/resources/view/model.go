package resourceview

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Triple-Whale/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
)

type CHView struct {
	Database string `ch:"database"`
	Name     string `ch:"name"`
	Query    string `ch:"as_select"`
	Engine   string `ch:"engine"`
	Comment  string `ch:"comment"`
}

type ViewResource struct {
	Database     string
	Name         string
	Query        string
	Cluster      string
	Materialized bool
	ToTable      string
	Comment      string
}

func (t *CHView) ToResource() (*ViewResource, error) {
	viewResource := ViewResource{
		Database: t.Database,
		Name:     t.Name,
		Query:    t.Query,
	}

	comment, cluster, toTable, err := common.UnmarshalComment(t.Comment)
	if err != nil {
		return nil, err
	}

	viewResource.Cluster = cluster
	viewResource.Comment = comment
	viewResource.ToTable = toTable
	viewResource.Materialized = t.Engine == "MaterializedView"

	return &viewResource, nil
}

func (t *ViewResource) Validate() diag.Diagnostics {
	var diags diag.Diagnostics

	re := regexp.MustCompile(`(?i)FROM\s+([a-zA-Z0-9_\-\.]+)`)

	matches := re.FindAllStringSubmatch(t.Query, -1)

	for _, match := range matches {
		if !strings.Contains(match[1], ".") {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "invalid value",
				Detail:   fmt.Sprintf("query table %s must be in database.table format", match[1]),
			})
		}
	}

	return diags
}
