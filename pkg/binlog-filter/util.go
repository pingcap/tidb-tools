/*************************************************************************
 *
 * PingCAP CONFIDENTIAL
 * __________________
 *
 *  [2015] - [2018] PingCAP Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of PingCAP Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to PingCAP Incorporated
 * and its suppliers and may be covered by P.R.China and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from PingCAP Incorporated.
 */

package filter

import "github.com/pingcap/tidb/ast"

// AstToDDLEvent returns filter.DDLEvent
func AstToDDLEvent(node ast.StmtNode) EventType {
	switch node.(type) {
	case *ast.CreateDatabaseStmt:
		return CreateDatabase
	case *ast.DropDatabaseStmt:
		return DropDatabase
	case *ast.CreateTableStmt:
		return CreateTable
	case *ast.DropTableStmt:
		return DropTable
	case *ast.TruncateTableStmt:
		return TruncateTable
	case *ast.RenameTableStmt:
		return RenameTable
	case *ast.CreateIndexStmt:
		return CreateIndex
	case *ast.DropIndexStmt:
		return DropIndex
	case *ast.AlterTableStmt:
		return AlertTable
	}

	return NullEvent
}
