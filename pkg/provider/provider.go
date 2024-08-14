package provider

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/common"
	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/datasources"
	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/resources"
	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/sdk"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func init() {
	// Set descriptions to support markdown syntax, this will be used in document generation
	// and the language server.
	schema.DescriptionKind = schema.StringMarkdown

	// Customize the content of descriptions when output. For example you can add defaults on
	// to the exported descriptions if present.
	// schema.SchemaDescriptionBuilder = func(s *schema.Schema) string {
	// 	desc := s.Description
	// 	if s.Default != nil {
	// 		desc += fmt.Sprintf(" Defaults to `%v`.", s.Default)
	// 	}
	// 	return strings.TrimSpace(desc)
	// }
}

func New(version string) func() *schema.Provider {
	return func() *schema.Provider {
		return &schema.Provider{
			Schema: map[string]*schema.Schema{
				"default_cluster": {
					Description: "Default cluster, if provided will be used when no cluster is provided",
					Type:        schema.TypeString,
					Optional:    true,
				},
				"username": {
					Description: "Clickhouse username with admin privileges",
					Type:        schema.TypeString,
					Optional:    true,
					DefaultFunc: schema.EnvDefaultFunc("TF_VAR_CLICKHOUSE_USERNAME", "default"),
				},
				"password": {
					Description: "Clickhouse user password with admin privileges",
					Type:        schema.TypeString,
					Optional:    true,
					Sensitive:   true,
					DefaultFunc: schema.EnvDefaultFunc("TF_VAR_CLICKHOUSE_PASSWORD", ""),
				},
				"host": {
					Description: "Clickhouse server URL",
					Type:        schema.TypeString,
					Required:    true,
					DefaultFunc: schema.EnvDefaultFunc("TF_VAR_CLICKHOUSE_HOST", "127.0.0.1"),
				},
				"port": {
					Description: "Clickhouse server native protocol port (TCP)",
					Type:        schema.TypeInt,
					Required:    true,
					DefaultFunc: schema.EnvDefaultFunc("TF_VAR_CLICKHOUSE_PORT", 9000),
				},
				"secure": {
					Description: "Clickhouse secure connection",
					Type:        schema.TypeBool,
					Optional:    true,
					Default:     false,
				},
			},
			DataSourcesMap: map[string]*schema.Resource{
				"clickhouse_dbs": datasources.DataSourceDbs(),
			},
			ResourcesMap: map[string]*schema.Resource{
				"clickhouse_db":    resources.ResourceDb(),
				"clickhouse_table": resources.ResourceTable(),
				"clickhouse_view":  resources.ResourceView(),
				"clickhouse_role":  resources.ResourceRole(),
				"clickhouse_user":  resources.ResourceUser(),
			},
			ConfigureContextFunc: configure(),
		}
	}
}

func configure() func(context.Context, *schema.ResourceData) (any, diag.Diagnostics) {
	return func(ctx context.Context, d *schema.ResourceData) (any, diag.Diagnostics) {
		host := d.Get("host").(string)
		port := d.Get("port").(int)
		username := d.Get("username").(string)
		password := d.Get("password").(string)
		secure := d.Get("secure").(bool)

		var TLSConfig *tls.Config
		// To use TLS it's necessary to set the TLSConfig field as not nil
		if secure {
			TLSConfig = &tls.Config{
				InsecureSkipVerify: false,
			}
		}
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", host, port)},
			Auth: clickhouse.Auth{
				Username: username,
				Password: password,
			},
			Debug: common.DebugEnabled,
			Debugf: func(format string, v ...any) {
				if common.DebugEnabled {
					fmt.Printf(format, v...)
				}
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 300,
			},
			TLS: TLSConfig,
		})

		var diags diag.Diagnostics

		if err != nil {
			return nil, diag.FromErr(fmt.Errorf("error connecting to clickhouse: %v", err))
		}

		if err := conn.Ping(ctx); err != nil {
			return nil, diag.FromErr(fmt.Errorf("ping clickhouse database: %w", err))
		}

		return &sdk.Client{Connection: conn}, diags
	}
}
