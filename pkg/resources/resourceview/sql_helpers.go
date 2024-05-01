package resourceview

import (
	"fmt"

	"github.com/Triple-Whale/terraform-provider-clickhouse/pkg/common"
)

func buildCreateOnClusterSentence(resource ViewResource) (query string) {
	clusterStatement := common.GetClusterStatement(resource.Cluster)

	ret := fmt.Sprintf(
		"CREATE VIEW %v.%v %v as (%s) COMMENT '%s'",
		resource.Database,
		resource.Name,
		clusterStatement,
		resource.Query,
		resource.Comment,
	)
	return ret
}
