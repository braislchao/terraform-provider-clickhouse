package common

import (
	"os"
	"strings"
)

var TFLogLevels = map[string]bool{
	"DEBUG": true,
	"TRACE": true,
	"JSON":  true,
}

var TFLogLevel string
var DebugEnabled bool

func init() {
	TFLogLevel = strings.ToUpper(os.Getenv("TF_LOG"))
	DebugEnabled = TFLogLevels[TFLogLevel]
}
