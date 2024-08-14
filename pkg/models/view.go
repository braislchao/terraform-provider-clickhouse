package models

import "github.com/hashicorp/terraform-plugin-sdk/v2/diag"

type ViewResource struct {
	Database     string
	Name         string
	Query        string
	Cluster      string
	Materialized bool
	ToTable      string
	Comment      string
}

type CHView struct {
	Database string `ch:"database"`
	Name     string `ch:"name"`
	Query    string `ch:"as_select"`
	Engine   string `ch:"engine"`
	Comment  string `ch:"comment"`
}

func (t *CHView) ToResource() (*ViewResource, error) {
	viewResource := ViewResource{
		Database: t.Database,
		Name:     t.Name,
		Query:    t.Query,
	}

	viewResource.Comment = t.Comment
	viewResource.Materialized = t.Engine == "MaterializedView"

	return &viewResource, nil
}

func (t *ViewResource) Validate() diag.Diagnostics {
	var diags diag.Diagnostics

	return diags
}
