package common

func MapArrayInterfaceToArrayOfStrings(in []interface{}) []string {
	ret := make([]string, 0)
	for _, s := range in {
		ret = append(ret, s.(string))
	}
	return ret
}

func MapInterfaceToMapOfString(in map[string]interface{}) map[string]string {
	ret := make(map[string]string)
	for key, value := range in {
		ret[key] = value.(string)
	}
	return ret
}
