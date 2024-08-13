package models

import (
	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type CHUser struct {
	Name  string   `ch:"name"`
	Roles []string `ch:"default_roles_list"`
}

type UserResource struct {
	Name     string
	Password string
	Roles    *schema.Set
}

func (u *CHUser) ToUserResource() *UserResource {
	return &UserResource{
		Name:  u.Name,
		Roles: common.StringListToSet(u.Roles),
	}
}

func (t *TableResource) SetColumns(columns []interface{}) {
	for _, column := range columns {
		columnDefinition := ColumnDefinition{
			Name:              column.(map[string]interface{})["name"].(string),
			Type:              column.(map[string]interface{})["type"].(string),
			Comment:           column.(map[string]interface{})["comment"].(string),
			DefaultKind:       column.(map[string]interface{})["default_kind"].(string),
			DefaultExpression: column.(map[string]interface{})["default_expression"].(string),
		}
		t.Columns = append(t.Columns, columnDefinition)
	}
}

func (t *TableResource) SetIndexes(indexes []interface{}) {
	for _, index := range indexes {
		indexDefinition := IndexDefinition{
			Name:        index.(map[string]interface{})["name"].(string),
			Expression:  index.(map[string]interface{})["expression"].(string),
			Type:        index.(map[string]interface{})["type"].(string),
			Granularity: uint64(index.(map[string]interface{})["granularity"].(int)),
		}
		t.Indexes = append(t.Indexes, indexDefinition)
	}
}
