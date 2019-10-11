// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package check

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
)

/*****************************************************/

// SourceDumpPrivilegeChecker checks dump privileges of source DB.
type SourceDumpPrivilegeChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewSourceDumpPrivilegeChecker returns a Checker.
func NewSourceDumpPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &SourceDumpPrivilegeChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
// We only check RELOAD, SELECT privileges.
func (pc *SourceDumpPrivilegeChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check dump privileges of source DB",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	grants, err := dbutil.ShowGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}

	verifyPrivileges(result, grants, []string{"RELOAD", "SELECT"})
	return result
}

// Name implements the Checker interface.
func (pc *SourceDumpPrivilegeChecker) Name() string {
	return "source db dump privilege chcker"
}

/*****************************************************/

// SourceReplicatePrivilegeChecker checks replication privileges of source DB.
type SourceReplicatePrivilegeChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewSourceReplicationPrivilegeChecker returns a Checker.
func NewSourceReplicationPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &SourceReplicatePrivilegeChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
// We only check REPLICATION SLAVE, REPLICATION CLIENT privileges.
func (pc *SourceReplicatePrivilegeChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check replication privileges of source DB",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	grants, err := dbutil.ShowGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}

	verifyPrivileges(result, grants, []string{"REPLICATION SLAVE", "REPLICATION CLIENT"})
	return result
}

// Name implements the Checker interface.
func (pc *SourceReplicatePrivilegeChecker) Name() string {
	return "source db replication privilege chcker"
}

func verifyPrivileges(result *Result, grants []string, expectedGrants []string) {
	if len(grants) == 0 {
		result.ErrorMsg = "there is no such grant defined for current user on host '%'"
		return
	}

	// TiDB parser does not support parse `IDENTIFIED BY PASSWORD <secret>`,
	// but it may appear in some cases, ref: https://bugs.mysql.com/bug.php?id=78888.
	// We do not need the password in grant statement, so we can replace it.
	firstGrant := strings.Replace(grants[0], "IDENTIFIED BY PASSWORD <secret>", "IDENTIFIED BY PASSWORD 'secret'", 1)

	// support parse `IDENTIFIED BY PASSWORD WITH {GRANT OPTION | resource_option} ...`
	firstGrant = strings.Replace(firstGrant, "IDENTIFIED BY PASSWORD WITH", "IDENTIFIED BY PASSWORD 'secret' WITH", 1)

	// support parse `IDENTIFIED BY PASSWORD`
	if strings.HasSuffix(firstGrant, "IDENTIFIED BY PASSWORD") {
		firstGrant = firstGrant + " 'secret'"
	}

	// get username and hostname
	node, err := parser.New().ParseOneStmt(firstGrant, "", "")
	if err != nil {
		result.ErrorMsg = errors.ErrorStack(errors.Annotatef(err, "grants[0] %s", grants[0]))
		return
	}
	grantStmt, ok := node.(*ast.GrantStmt)
	if !ok {
		result.ErrorMsg = fmt.Sprintf("%s is not grant statment", grants[0])
		return
	}

	if len(grantStmt.Users) == 0 {
		result.ErrorMsg = fmt.Sprintf("grant has not user %s", grantStmt.Text())
		return
	}

	// TODO: user tidb parser(which not works very well now)
	lackOfPrivileges := make([]string, 0, len(expectedGrants))
	for _, expected := range expectedGrants {
		hasPrivilege := false
		for _, grant := range grants {
			if strings.Contains(grant, "ALL PRIVILEGES") {
				result.State = StateSuccess
				return
			}
			if strings.Contains(grant, expected) {
				hasPrivilege = true
			}
		}
		if !hasPrivilege {
			lackOfPrivileges = append(lackOfPrivileges, expected)
		}
	}

	user := grantStmt.Users[0]
	if len(lackOfPrivileges) != 0 {
		privileges := strings.Join(lackOfPrivileges, ",")
		result.ErrorMsg = fmt.Sprintf("lack of %s privilege", privileges)
		result.Instruction = fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%s';", privileges, user.User.Username, "%")
		return
	}

	result.State = StateSuccess
	return
}
