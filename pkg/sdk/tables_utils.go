package sdk

import (
	"fmt"
	"strings"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/common"
	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
)

func buildColumnsSentence(cols []models.ColumnDefinition) []string {
	outColumn := make([]string, 0)
	for _, col := range cols {
		outColumn = append(outColumn, fmt.Sprintf("\t `%s` %s %s %s %s %s", col.Name, col.Type, col.DefaultKind, col.DefaultExpression, col.CompressionCodec, getComment(col.Comment)))
	}
	return outColumn
}

func buildIndexesSentence(indexes []models.IndexDefinition) []string {
	outIndexes := make([]string, 0)
	for _, index := range indexes {
		indexStatement := fmt.Sprintf("INDEX %s %s TYPE %s", index.Name, index.Expression, index.Type)
		if index.Granularity > 0 {
			indexStatement += fmt.Sprintf(" GRANULARITY %d", index.Granularity)
		}
		outIndexes = append(outIndexes, fmt.Sprintf("\t%s", indexStatement))
	}
	return outIndexes
}

func getComment(comment string) string {
	if comment != "" {
		return fmt.Sprintf("COMMENT '%s'", comment)
	}
	return ""
}

func buildPartitionBySentence(partitionBy []models.PartitionByResource) string {
	if len(partitionBy) > 0 {
		partitionBySentenceItems := make([]string, 0)
		for _, partitionByItem := range partitionBy {
			if partitionByItem.PartitionFunction == "" {
				partitionBySentenceItems = append(partitionBySentenceItems, partitionByItem.By)
			} else if partitionByItem.Mod == "" {
				partitionBySentenceItems = append(partitionBySentenceItems, fmt.Sprintf("%v(%v)", partitionByItem.PartitionFunction, partitionByItem.By))
			} else {
				partitionBySentenceItems = append(partitionBySentenceItems, fmt.Sprintf("%v(%v) %% %v", partitionByItem.PartitionFunction, partitionByItem.By, partitionByItem.Mod))
			}
		}
		return fmt.Sprintf("PARTITION BY (%v)", strings.Join(partitionBySentenceItems, ", "))
	}
	return ""
}
func buildPrimaryKeySentence(primaryKey []string) string {
	if len(primaryKey) > 0 {
		return fmt.Sprintf("PRIMARY KEY (%v)", strings.Join(primaryKey, ", "))
	}
	return ""
}

func buildOrderBySentence(orderBy []string) string {
	if len(orderBy) > 0 {
		return fmt.Sprintf("ORDER BY (%v)", strings.Join(orderBy, ", "))
	}
	return ""
}

func buildSettingsSentence(settings map[string]string) string {
	if len(settings) > 0 {
		settingsList := make([]string, 0)
		for key, value := range settings {
			settingsList = append(settingsList, fmt.Sprintf("%s = '%s'", key, value))
		}
		ret := fmt.Sprintf("SETTINGS %s", strings.Join(settingsList, ", "))
		return ret
	}
	return ""
}

func buildTTLSentence(ttl map[string]string) string {
	if len(ttl) > 0 {
		ttlList := make([]string, 0)
		for key, value := range ttl {
			ttlList = append(ttlList, fmt.Sprintf("%s %s", key, value))
		}
		ret := fmt.Sprintf("TTL %s", strings.Join(ttlList, ", "))
		return ret
	}
	return ""
}

func buildCreateTableOnClusterSentence(resource models.TableResource) (query string) {
	createStatement := common.GetCreateStatement("table")
	clusterStatement := common.GetClusterStatement(resource.Cluster)

	// Build columns and indexes
	columns := []string{}

	if len(resource.Columns) > 0 {
		columns = append(columns, buildColumnsSentence(resource.GetColumnsResourceList())...)
	}

	if len(resource.Indexes) > 0 {
		columns = append(columns, buildIndexesSentence(resource.Indexes)...)
	}

	columnsStatement := ""
	if len(columns) > 0 {
		columnsStatement = fmt.Sprintf("(\n%s\n)", strings.Join(columns, ",\n"))
	}

	ret := fmt.Sprintf(
		"%s %v.%v %v %v ENGINE = %v(%v) %s %s %s %s %s COMMENT '%s'",
		createStatement,
		resource.Database,
		resource.Name,
		clusterStatement,
		columnsStatement,
		resource.Engine,
		strings.Join(resource.EngineParams, ", "),
		buildOrderBySentence(resource.OrderBy),
		buildPrimaryKeySentence(resource.PrimaryKey),
		buildPartitionBySentence(resource.PartitionBy),
		buildTTLSentence(resource.TTL),
		buildSettingsSentence(resource.Settings),
		resource.Comment,
	)

	return ret
}

