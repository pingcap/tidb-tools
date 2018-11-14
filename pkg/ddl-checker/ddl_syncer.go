package ddl_checker

import (
	"context"
	"database/sql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

type DDLSyncer struct {
	db *sql.DB
	ec *ExecutableChecker
}

func NewDDLSyncer(cfg *dbutil.DBConfig, executableChecker *ExecutableChecker) (*DDLSyncer, error) {
	db, err := dbutil.OpenDB(*cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &DDLSyncer{db, executableChecker}, nil
}

func (ds *DDLSyncer) SyncTable(schemaName string, tableName string) error {
	err := ds.ec.DropTable(tableName)
	if err != nil {
		return errors.Trace(err)
	}
	createTableSQL, err := dbutil.GetCreateTableSQL(context.Background(), ds.db, schemaName, tableName)
	if err != nil {
		return errors.Trace(err)
	}
	err = ds.ec.Execute(createTableSQL)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (ds *DDLSyncer) Close() error {
	err1 := ds.ec.Close()
	err2 := dbutil.CloseDB(ds.db)
	if err1 != nil {
		return errors.Trace(err1)
	}
	if err2 != nil {
		return errors.Trace(err2)
	}
	return nil
}
