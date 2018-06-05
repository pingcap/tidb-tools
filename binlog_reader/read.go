// Copyright 2016 PingCAP, Inc.
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

package main

import (
	"io"
	"os"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/binlog_reader/util"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
	pb "github.com/pingcap/tipb/go-binlog"
)

var (
	// PrivateFileMode is the permission for service file
	PrivateFileMode os.FileMode = 0600
)

type BinlogReader struct {
	FileName string
	Schema   *util.Schema
}

// NewBinlogReader returns a BinlogReader
func NewBinlogReader(filename string, etcdURLS string) (*BinlogReader, error) {
	b := &BinlogReader{
		FileName: filename,
	}
	startTs, err := b.GetStartTs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("binlog %s's start ts: %d", filename, startTs)

	// TODO: use startTs load history ddl jobs when handle ddl is ok
	// jobs, err := util.LoadHistoryDDLJobs(etcdURLS, startTs)
	jobs, err := util.LoadHistoryDDLJobs(etcdURLS, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("get %d history jobs", len(jobs))

	b.Schema, err = util.NewSchema(jobs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return b, nil
}

// Walk reads binlog file
func (b *BinlogReader) Walk() error {
	var ent = &pb.Entity{}
	var decoder util.Decoder

	f, err := os.OpenFile(b.FileName, os.O_RDONLY, PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	from := pb.Pos{
		Suffix: 0,
		Offset: 0,
	}
	decoder = util.NewDecoder(from, io.Reader(f))

	for {
		buf := util.BinlogBufferPool.Get().(*util.BinlogBuffer)
		err = decoder.Decode(ent, buf)
		if err != nil {
			break
		}

		binlog := new(pb.Binlog)
		err = binlog.Unmarshal(ent.Payload)
		if err != nil {
			return errors.Trace(err)
		}

		startTs := binlog.GetStartTs()
		commitTs := binlog.GetCommitTs()
		tp := binlog.GetTp()
		preWriteValue := binlog.GetPrewriteValue()
		log.Infof("start ts: %d, commit ts: %d, tp: %v", startTs, commitTs, tp)

		preWriteValue = binlog.GetPrewriteValue()
		preWrite := &pb.PrewriteValue{}
		err = preWrite.Unmarshal(preWriteValue)
		if err != nil {
			return errors.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
		}
		err = b.translateSqls(preWrite.GetMutations())

		if binlog.GetDdlQuery() != nil {
			log.Infof("ddl sql: %s", string(binlog.GetDdlQuery()))
		}

		util.BinlogBufferPool.Put(buf)
	}

	if err != nil && err != io.EOF {
		return errors.Trace(err)
	}

	return nil
}

// GetStartTs get the first start ts in binlog file
func (b *BinlogReader) GetStartTs() (int64, error) {
	var ent = &pb.Entity{}
	var decoder util.Decoder

	f, err := os.OpenFile(b.FileName, os.O_RDONLY, PrivateFileMode)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer f.Close()

	from := pb.Pos{
		Suffix: 0,
		Offset: 0,
	}
	decoder = util.NewDecoder(from, io.Reader(f))

	buf := util.BinlogBufferPool.Get().(*util.BinlogBuffer)
	err = decoder.Decode(ent, buf)
	if err != nil {
		return 0, errors.Trace(err)
	}

	binlog := new(pb.Binlog)
	err = binlog.Unmarshal(ent.Payload)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return binlog.StartTs, nil
}

func (b *BinlogReader) translateSqls(mutations []pb.TableMutation) error {
	for _, mutation := range mutations {
		var (
			err  error
			sqls = make(map[pb.MutationType][]string)

			// the restored sqls's args, ditto
			args = make(map[pb.MutationType][][]string)

			// the offset of specified type sql
			offsets = make(map[pb.MutationType]int)

			// the binlog dml sort
			sequences = mutation.GetSequence()
		)
		tableInfo, ok := b.Schema.TableByID(mutation.GetTableId())
		if !ok {
			log.Errorf("can't find table id %d", mutation.GetTableId())
			continue
		}
		schemaName, tableName, ok := b.Schema.SchemaAndTableName(mutation.GetTableId())
		if !ok {
			log.Infof("can't find schema name and table name of table id %d", schemaName, tableName)
			continue
		}

		if len(mutation.GetInsertedRows()) > 0 {
			sqls[pb.MutationType_Insert], args[pb.MutationType_Insert], err = util.GenInsertSQLs(schemaName, tableInfo, mutation.GetInsertedRows())
			if err != nil {
				return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_Insert] = 0
		}

		if len(mutation.GetUpdatedRows()) > 0 {
			sqls[pb.MutationType_Update], args[pb.MutationType_Update], err = util.GenUpdateSQLs(schemaName, tableInfo, mutation.GetUpdatedRows())
			if err != nil {
				return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_Update] = 0
		}

		if len(mutation.GetDeletedRows()) > 0 {
			sqls[pb.MutationType_DeleteRow], args[pb.MutationType_DeleteRow], err = util.GenDeleteSQLs(schemaName, tableInfo, mutation.GetDeletedRows())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_DeleteRow] = 0
		}

		for _, dmlType := range sequences {
			if offsets[dmlType] >= len(sqls[dmlType]) {
				return errors.Errorf("gen sqls failed: sequence %v execution %s sqls %v", sequences, dmlType, sqls[dmlType])
			}

			log.Infof("dml sql: %s, args: %s", sqls[dmlType][offsets[dmlType]], args[dmlType][offsets[dmlType]])
			offsets[dmlType] = offsets[dmlType] + 1
		}
	}
	return nil
}

func (b *BinlogReader) handleDDL(job *model.Job) (string, string, string, error) {
	if job.State == model.JobStateCancelled {
		return "", "", "", nil
	}

	log.Infof("ddl query %s", job.Query)
	sql := job.Query
	if sql == "" {
		return "", "", "", errors.Errorf("[ddl job sql miss]%+v", job)
	}

	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo

		err := b.Schema.CreateSchema(schema)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, "", sql, nil

	case model.ActionDropSchema:
		schemaName, err := b.Schema.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schemaName, "", sql, nil

	case model.ActionRenameTable:
		// ignore schema doesn't support reanme ddl
		_, ok := b.Schema.SchemaByTableID(job.TableID)
		if !ok {
			return "", "", "", errors.NotFoundf("table(%d) or it's schema", job.TableID)
		}

		// first drop the table
		_, err := b.Schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}
		// create table
		table := job.BinlogInfo.TableInfo
		schema, ok := b.Schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = b.Schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, table.Name.O, sql, nil

	case model.ActionCreateTable:
		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := b.Schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := b.Schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, table.Name.O, sql, nil

	case model.ActionDropTable:
		schema, ok := b.Schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		tableName, err := b.Schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, tableName, sql, nil

	case model.ActionTruncateTable:
		schema, ok := b.Schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		_, err := b.Schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		err = b.Schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, table.Name.O, sql, nil

	default:

		binlogInfo := job.BinlogInfo
		if binlogInfo == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}
		tableInfo := binlogInfo.TableInfo
		if tableInfo == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := b.Schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := b.Schema.ReplaceTable(tableInfo)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, tableInfo.Name.O, sql, nil
	}
}

func toColumnTypeMap(columns []*model.ColumnInfo) map[int64]*types.FieldType {
	colTypeMap := make(map[int64]*types.FieldType)
	for _, col := range columns {
		colTypeMap[col.ID] = &col.FieldType
	}

	return colTypeMap
}
