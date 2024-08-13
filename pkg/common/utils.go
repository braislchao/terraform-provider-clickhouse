package common

import (
	"os"

	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func GetClusterStatement(cluster string) (clusterStatement string) {
	if cluster != "" {
		return fmt.Sprintf("ON CLUSTER %s", cluster)
	}
	return ""
}

// Quote all strings on a string slice
func Quote(elems []string) []string {
	var quotedElems []string
	for _, elem := range elems {
		quotedElems = append(quotedElems, fmt.Sprintf("%q", elem))
	}
	return quotedElems
}

func StringSetToList(set *schema.Set) []string {
	var list []string
	for _, item := range set.List() {
		list = append(list, item.(string))
	}
	return list
}

func StringListToSet(list []string) *schema.Set {
	var set []interface{}
	for _, item := range list {
		set = append(set, item)
	}
	return schema.NewSet(schema.HashString, set)
}

// NormalizeQuery converts a regular query to lowercase, and strips new lines.
// This ensures that queries stored in the state are consistent with the queries
// stored in Clickhouse's `system.tables`
func NormalizeQuery(query string) string {
	query = strings.ToLower(query)
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.Join(strings.Fields(query), " ")

	return query
}

func GetCreateStatement(resourceType string) string {
	resourceType = strings.ToUpper(resourceType)
	isDatabase := resourceType == "DATABASE"

	if IsEnvTrue("TF_VAR_CREATE_OR_REPLACE") && !isDatabase {
		return fmt.Sprintf("CREATE OR REPLACE %s", resourceType)
	}

	if IsEnvTrue("TF_VAR_CREATE_IF_NOT_EXISTS") {
		return fmt.Sprintf("CREATE %s IF NOT EXISTS", resourceType)
	}

	return fmt.Sprintf("CREATE %s", resourceType)
}

func IsEnvTrue(envVar string) bool {
	val, ok := os.LookupEnv(envVar)
	return ok && strings.ToLower(val) == "true"
}
