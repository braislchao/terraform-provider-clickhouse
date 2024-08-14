package sdk

import (
	"context"
	"fmt"
	"strings"

	"github.com/FlowdeskMarkets/terraform-provider-clickhouse/pkg/models"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func getGrantQuery(roleName string, privileges []string, database string) string {
	if database == "system" || database == "*" {
		return fmt.Sprintf("GRANT CURRENT GRANTS (%s ON %s.*) TO %s", strings.Join(privileges, ","), database, roleName)
	}
	return fmt.Sprintf("GRANT %s ON %s.* TO %s", strings.Join(privileges, ","), database, roleName)
}

func (c *Client) getRoleGrants(ctx context.Context, roleName string) ([]models.CHGrant, error) {
	query := fmt.Sprintf("SELECT role_name, access_type, database FROM system.grants WHERE role_name = '%s'", roleName)
	rows, err := c.Conn.Query(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("error fetching role grants: %s", err)
	}

	var privileges []models.CHGrant
	for rows.Next() {
		var privilege models.CHGrant
		err := rows.ScanStruct(&privilege)
		if err != nil {
			return nil, fmt.Errorf("error scanning role grant: %s", err)
		}
		if privilege.Database == "" {
			privilege.Database = "*"
		}
		privileges = append(privileges, privilege)
	}

	return privileges, nil
}

func (c *Client) GetRole(ctx context.Context, roleName string) (*models.CHRole, error) {
	roleQuery := fmt.Sprintf("SELECT name FROM system.roles WHERE name = '%s'", roleName)

	rows, err := c.Conn.Query(ctx, roleQuery)
	if err != nil {
		return nil, fmt.Errorf("error fetching role: %s", err)
	}
	if !rows.Next() {
		return nil, nil
	}

	privileges, err := c.getRoleGrants(ctx, roleName)
	if err != nil {
		return nil, fmt.Errorf("error fetching role grants: %s", err)
	}

	return &models.CHRole{
		Name:       roleName,
		Privileges: privileges,
	}, nil
}

func (c *Client) UpdateRole(ctx context.Context, rolePlan models.RoleResource, resourceData *schema.ResourceData) (*models.CHRole, error) {
	stateRoleName, _ := resourceData.GetChange("name")
	chRole, err := c.GetRole(ctx, stateRoleName.(string))

	if err != nil {
		return nil, fmt.Errorf("error fetching role: %s", err)
	}
	if chRole == nil {
		return nil, fmt.Errorf("role %s not found", rolePlan.Name)
	}

	roleNameHasChange := resourceData.HasChange("name")
	roleDatabaseHasChange := resourceData.HasChange("database")
	rolePrivilegesHasChange := resourceData.HasChange("privileges")

	var grantPrivileges []string
	var revokePrivileges []string
	if rolePrivilegesHasChange {
		for _, planPrivilege := range rolePlan.Privileges.List() {
			found := false
			for _, privilege := range chRole.Privileges {
				if privilege.AccessType == planPrivilege {
					found = true
				}
			}
			if !found {
				grantPrivileges = append(grantPrivileges, planPrivilege.(string))
			}
		}

		for _, privilege := range chRole.Privileges {
			if !rolePlan.Privileges.Contains(privilege.AccessType) {
				revokePrivileges = append(revokePrivileges, privilege.AccessType)
			}
		}
	}

	if roleNameHasChange {
		err := c.Conn.Exec(ctx, fmt.Sprintf("ALTER ROLE %s RENAME TO %s", chRole.Name, rolePlan.Name))
		if err != nil {
			return nil, fmt.Errorf("error renaming role %s to %s: %v", chRole.Name, rolePlan.Name, err)
		}
	}

	if roleDatabaseHasChange {
		err := c.Conn.Exec(ctx, fmt.Sprintf("REVOKE ALL ON *.* FROM %s", rolePlan.Name))
		if err != nil {
			return nil, fmt.Errorf("error revoking all privileges from role %s: %v", chRole.Name, err)
		}
		dbPrivileges := chRole.GetPrivilegesList()
		err = c.Conn.Exec(ctx, getGrantQuery(
			rolePlan.Name,
			dbPrivileges,
			rolePlan.Database,
		))
		if err != nil {
			return nil, fmt.Errorf("error granting privileges to role %s: %v", chRole.Name, err)
		}
	}

	if len(grantPrivileges) > 0 {
		err := c.Conn.Exec(ctx, getGrantQuery(rolePlan.Name, grantPrivileges, rolePlan.Database))
		if err != nil {
			return nil, fmt.Errorf("error granting privileges to role %s: %v", chRole.Name, err)
		}
	}

	if len(revokePrivileges) > 0 {
		err := c.Conn.Exec(ctx, fmt.Sprintf("REVOKE %s ON %s.* FROM %s", strings.Join(revokePrivileges, ","), rolePlan.Database, rolePlan.Name))
		if err != nil {
			return nil, fmt.Errorf("error revoking privileges from role %s: %v", chRole.Name, err)
		}
	}

	return c.GetRole(ctx, rolePlan.Name)
}

func (c *Client) CreateRole(ctx context.Context, name string, database string, privileges []string) (*models.CHRole, error) {
	err := c.Conn.Exec(ctx, fmt.Sprintf("CREATE ROLE %s", name))
	if err != nil {
		return nil, fmt.Errorf("error creating role: %s", err)
	}

	var chPrivileges []models.CHGrant

	for _, privilege := range privileges {
		err = c.Conn.Exec(ctx, getGrantQuery(name, []string{privilege}, database))
		if err != nil {
			// Rollback
			err2 := c.Conn.Exec(ctx, fmt.Sprintf("DROP ROLE %s", name))
			if err2 != nil {
				return nil, fmt.Errorf("error creating role: %s:%s", err, err2)
			}
			return nil, fmt.Errorf("error creating role: %s", err)
		}
		chPrivileges = append(chPrivileges, models.CHGrant{RoleName: name, AccessType: privilege, Database: database})
	}
	return &models.CHRole{Name: name, Privileges: chPrivileges}, nil
}

func (c *Client) DeleteRole(ctx context.Context, name string) error {
	return c.Conn.Exec(ctx, fmt.Sprintf("DROP ROLE %s", name))
}
