package resourceview

import (
	"github.com/Triple-Whale/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
)

type CHView struct {
	Database string `ch:"database"`
	Name     string `ch:"name"`
	Query    string `ch:"as_select"`
	Comment  string `ch:"comment"`
}

type ViewResource struct {
	Database string
	Name     string
	Query    string
	Cluster  string
	Comment  string
}

func (t *CHView) ToResource() (*ViewResource, error) {
	viewResource := ViewResource{
		Database: t.Database,
		Name:     t.Name,
		Query:    t.Query,
	}

	comment, cluster, err := common.UnmarshalComment(t.Comment)
	if err != nil {
		return nil, err
	}

	viewResource.Cluster = cluster
	viewResource.Comment = comment

	return &viewResource, nil
}

func (t *ViewResource) Validate(diags diag.Diagnostics) {
	// TODO
}
