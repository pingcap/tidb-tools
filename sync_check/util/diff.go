package util

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	db1          *sql.DB
	db2          *sql.DB
	timeField    string
	beginTime    string
	endTime      string
	splitField   string
	dbName       string
	tables       []string
	chunkSize    int
	sample       int
	checkThCount int
}

// NewDiff returns a Diff instance.
func NewDiff(db1, db2 *sql.DB, dbName, timeField, beginTime, endTime, 
	splitField string, chunkSize, sample, checkThCount int, tables []string) *Diff {
	return &Diff{
		db1:          db1,
		db2:          db2,
		timeField:    timeField,
		beginTime:    beginTime,
		endTime:      endTime,
		splitField:   splitField,
		dbName:       dbName,
		tables:       tables,
		chunkSize:    chunkSize,
		sample:       sample,
		checkThCount: checkThCount,
	}
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (equal bool, err error) {
	equal = true
	tbls1, err := getTables(df.db1)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	tbls2, err := getTables(df.db2)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	eq := equalStrings(tbls1, tbls2)
	if !eq {
		log.Infof("show tables get different table. [source db tables] %v [target db tables] %v", tbls1, tbls2)
		equal = false
	}

	checkTables := make([]string, 0, len(tbls1))
	if len(df.tables) == 0 {
		checkTables = tbls1
	} else {
		checkTables = df.tables
	}

	for _, tblName := range checkTables {
		eq, err = df.EqualIndex(tblName)
		if err != nil {
			err = errors.Trace(err)
			return
		}
		if !eq {
			log.Infof("table have different index: %s\n", tblName)
			equal = false
		}

		eq, err = df.EqualTable(tblName)
		if err != nil || !eq {
			err = errors.Trace(err)
			equal = false
			return
		}
	}

	return
}

// EqualTable tests whether two database table have same data and schema.
func (df *Diff) EqualTable(tblName string) (bool, error) {
	eq, err := df.equalCreateTable(tblName)
	if err != nil {
		return eq, errors.Trace(err)
	}
	if !eq {
		log.Infof("table have different schema: %s\n", tblName)
		return eq, err
	}

	eq, err = df.equalTableData(tblName)
	if err != nil {
		return eq, errors.Trace(err)
	}
	if !eq {
		log.Infof("table data different: %s\n", tblName)
	}
	return eq, err
}

// EqualIndex tests whether two database index are same.
func (df *Diff) EqualIndex(tblName string) (bool, error) {
	index1, err := getTableIndex(df.db1, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer index1.Close()
	index2, err := getTableIndex(df.db2, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer index2.Close()

	eq, err := equalRows(index1, index2, &showIndex{}, &showIndex{})
	if err != nil || !eq {
		return eq, errors.Trace(err)
	}
	return eq, nil
}

func (df *Diff) equalCreateTable(tblName string) (bool, error) {
	_, err1 := getCreateTable(df.db1, tblName)
	_, err2 := getCreateTable(df.db2, tblName)

	if errors.IsNotFound(err1) && errors.IsNotFound(err2) {
		return true, nil
	}
	if err1 != nil {
		return false, errors.Trace(err1)
	}
	if err2 != nil {
		return false, errors.Trace(err2)
	}

	// TODO ignore table schema currently
	// return table1 == table2, nil
	return true, nil
}

func (df *Diff) equalTableData(tblName string) (bool, error) {
	dumpJobs, err := generateDumpJob(df.db1, df.dbName, tblName, df.timeField, df.beginTime, df.endTime, df.splitField, df.chunkSize, df.sample)
	if err != nil {
		return false, errors.Trace(err)
	}

	checkNums := len(dumpJobs) * df.sample / 100
	if checkNums == 0 {
		checkNums = 1
	}
	checkNumArr := getRandomN(len(dumpJobs), checkNums)
	log.Infof("total has %d check jobs, check %+v", len(dumpJobs), checkNumArr)

	checkResultCh := make(chan bool, df.checkThCount)
	defer close(checkResultCh)

	for i := 0; i < df.checkThCount; i++ {
		checkJobs := make([]*dumpJob, 0, checkNums)
		for j := checkNums*i/df.checkThCount; j < checkNums*(i+1)/df.checkThCount; j++ {
			checkJobs = append(checkJobs, dumpJobs[j])
		}
		go df.checkChunkDataEqual(checkJobs, tblName, checkResultCh)
	}

	num := 0
	for {
		select {
		case eq := <-checkResultCh:
			num++
			if !eq {
				return false, nil
			}
			if num == df.checkThCount {
				return true, nil
			}
		}
	}
}

func (df *Diff) checkChunkDataEqual(dumpJobs []*dumpJob, tblName string, checkResultCh chan bool) {
	if len(dumpJobs) == 0 {
		checkResultCh <- true
		return
	}

	for _, job := range dumpJobs {
		log.Infof("table: %s, where: %s", job.table, job.where)

		rows1, err := getChunkRows(df.db1, df.dbName, tblName, job.where)
		if err != nil {
			checkResultCh <- false
			log.Errorf("checkChunkDataEqual error %+v", err)
			return
		}
		defer rows1.Close()

		rows2, err := getChunkRows(df.db2, df.dbName, tblName, job.where)
		if err != nil {
			checkResultCh <- false
			log.Errorf("checkChunkDataEqual error %+v", err)
			return
		}
		defer rows2.Close()

		cols1, err := rows1.Columns()
		if err != nil {
			checkResultCh <- false
			log.Errorf("checkChunkDataEqual error %+v", err)
			return
		}
		cols2, err := rows2.Columns()
		if err != nil {
			checkResultCh <- false
			log.Errorf("checkChunkDataEqual error %+v", err)
			return
		}
		if len(cols1) != len(cols2) {
			checkResultCh <- false
			log.Errorf("checkChunkDataEqual error %+v", err)
			return
		}

		row1 := make(rawBytesRow, len(cols1))
		row2 := make(rawBytesRow, len(cols2))
		eq, err := equalRows(rows1, rows2, row1, row2)
		if err != nil {
			checkResultCh <- false
			log.Errorf("checkChunkDataEqual error %+v", err)
			return
		}
		if !eq {
			checkResultCh <- false
			return
		}
	}

	checkResultCh <- true
	return
}

func equalRows(rows1, rows2 *sql.Rows, row1, row2 comparableSQLRow) (bool, error) {
	dataEqual := true
	for rows1.Next() {
		if !rows2.Next() {
			// rows2 count less than rows1
			log.Error("rows count different")
			return false, nil
		}

		eq, err := equalOneRow(rows1, rows2, row1, row2)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !eq {
			dataEqual = false
		}
	}
	if rows2.Next() {
		// rows1 count less than rows2
		log.Error("rows count different")
		return false, nil
	}
	if !dataEqual {
		log.Error("rows count equal, but not every row equal!")
		return false, nil
	}
	return true, nil
}

func equalOneRow(rows1, rows2 *sql.Rows, row1, row2 comparableSQLRow) (bool, error) {
	err := row1.Scan(rows1)
	if err != nil {
		return false, errors.Trace(err)
	}

	row2.Scan(rows2)
	if err != nil {
		return false, errors.Trace(err)
	}
	if row1.Equal(row2) {
		return true, nil
	}

	log.Errorf("row1 %s not equal row2 %s!", row1, row2)
	return false, nil
}

func getChunkRows(db *sql.DB, dbName, tblName string, where string) (*sql.Rows, error) {
	descs, err := getTableSchema(db, tblName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pk1 := orderbyKey(descs)

	query := fmt.Sprintf("SELECT %s * FROM `%s`.`%s` WHERE %s ORDER BY %s",
		"/*!40001 SQL_NO_CACHE */", dbName, tblName, where, pk1)

	rows, err := querySQL(db, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func getTableIndex(db *sql.DB, tblName string) (*sql.Rows, error) {
	rows, err := querySQL(db, fmt.Sprintf("show index from %s;", tblName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func getTables(db *sql.DB) ([]string, error) {
	rs, err := querySQL(db, "show tables;")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rs.Close()

	var tbls []string
	for rs.Next() {
		var name string
		err := rs.Scan(&name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tbls = append(tbls, name)
	}
	return tbls, nil
}

func getCreateTable(db *sql.DB, tn string) (string, error) {
	stmt := fmt.Sprintf("show create table %s;", tn)
	rs, err := querySQL(db, stmt)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rs.Close()

	if rs.Next() {
		var (
			name string
			cs   string
		)
		err := rs.Scan(&name, &cs)
		return cs, errors.Trace(err)
	}
	return "", errors.NewNotFound(nil, "table not exist")
}

type comparableSQLRow interface {
	sqlRow
	comparable
}

type sqlRow interface {
	Scan(*sql.Rows) error
}

type comparable interface {
	Equal(comparable) bool
}

type rawBytesRow []sql.RawBytes

func (r rawBytesRow) Len() int {
	return len([]sql.RawBytes(r))
}

func (r rawBytesRow) Scan(rows *sql.Rows) error {
	args := make([]interface{}, len(r))
	for i := 0; i < len(args); i++ {
		args[i] = &r[i]
	}

	err := rows.Scan(args...)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r rawBytesRow) Equal(data comparable) bool {
	r2, ok := data.(rawBytesRow)
	if !ok {
		return false
	}
	if r.Len() != r2.Len() {
		return false
	}
	for i := 0; i < r.Len(); i++ {
		if bytes.Compare(r[i], r2[i]) != 0 {
			return false
		}
	}
	return true
}

type showIndex struct {
	Table        sql.RawBytes
	NonUnique    sql.RawBytes
	KeyName      sql.RawBytes
	SeqInIndex   sql.RawBytes
	ColumnName   sql.RawBytes
	Collation    sql.RawBytes
	Cardinality  sql.RawBytes
	SubPart      sql.RawBytes
	Packed       sql.RawBytes
	Null         sql.RawBytes
	IndexType    sql.RawBytes
	Comment      sql.RawBytes
	IndexComment sql.RawBytes
}

func (si *showIndex) Scan(rows *sql.Rows) error {
	err := rows.Scan(&si.Table,
		&si.NonUnique,
		&si.KeyName,
		&si.SeqInIndex,
		&si.ColumnName,
		&si.Collation,
		&si.Cardinality,
		&si.SubPart,
		&si.Packed,
		&si.Null,
		&si.IndexType,
		&si.Comment,
		&si.IndexComment)
	return errors.Trace(err)
}

func (si *showIndex) Equal(data comparable) bool {
	si1, ok := data.(*showIndex)
	if !ok {
		return false
	}
	return bytes.Compare(si.Table, si1.Table) == 0 &&
		bytes.Compare(si.NonUnique, si1.NonUnique) == 0 &&
		bytes.Compare(si.KeyName, si1.KeyName) == 0 &&
		bytes.Compare(si.SeqInIndex, si1.SeqInIndex) == 0 &&
		bytes.Compare(si.ColumnName, si1.ColumnName) == 0 &&
		bytes.Compare(si.SubPart, si1.SubPart) == 0 &&
		bytes.Compare(si.Packed, si1.Packed) == 0
}

type describeTable struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default interface{}
	Extra   interface{}
}

func (desc *describeTable) Scan(rows *sql.Rows) error {
	err := rows.Scan(&desc.Field, &desc.Type, &desc.Null, &desc.Key, &desc.Default, &desc.Extra)
	return errors.Trace(err)
}

func getTableSchema(db *sql.DB, tblName string) ([]describeTable, error) {
	stmt := fmt.Sprintf("describe %s;", tblName)
	rows, err := querySQL(db, stmt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var descs []describeTable
	for rows.Next() {
		var desc describeTable
		err1 := desc.Scan(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		descs = append(descs, desc)
	}
	return descs, err
}

func orderbyKey(descs []describeTable) string {
	// TODO can't get the real primary key
	var buf bytes.Buffer
	firstTime := true
	for _, desc := range descs {
		if desc.Key == "PRI" {
			if firstTime {
				fmt.Fprintf(&buf, "%s", desc.Field)
				firstTime = false
			} else {
				fmt.Fprintf(&buf, ",%s", desc.Field)
			}
		}
	}
	if buf.Len() == 0 {
		// if no primary key found, use all fields as order by key
		for _, desc := range descs {
			if firstTime {
				fmt.Fprintf(&buf, "%s", desc.Field)
				firstTime = false
			} else {
				fmt.Fprintf(&buf, ",%s", desc.Field)
			}
		}
	}
	return buf.String()
}

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	log.Debugf("[query][sql]%s", query)

	rows, err = db.Query(query)

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	return rows, nil

}

func equalStrings(str1, str2 []string) bool {
	if len(str1) != len(str2) {
		return false
	}
	for i := 0; i < len(str1); i++ {
		if str1[i] != str2[i] {
			return false
		}
	}
	return true
}

// ShowDatabases returns a database lists.
func ShowDatabases(db *sql.DB) ([]string, error) {
	var ret []string
	rows, err := querySQL(db, "show databases;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, dbName)
	}
	return ret, nil
}

func getRandomN(total, num int) []int {
	if num > total {
		return nil
	}

	totalArray := make([]int, 0, total)
	for i := 0; i < total; i++ {
		totalArray = append(totalArray, i)
	}

	for j := 0; j < num; j++ {
		r := j + rand.Intn(total-j)
		totalArray[j], totalArray[r] = totalArray[r], totalArray[j]
	}

	return totalArray[:num]
}
