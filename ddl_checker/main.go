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

package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb-tools/pkg/ddl-checker"
	"os"
	"strconv"
	"strings"
)

var (
	modCode                                          = 0
	executableChecker *ddl_checker.ExecutableChecker = nil
	reader            *bufio.Reader                  = nil
	db                *sql.DB                        = nil

	host     = flag.String("host", "127.0.0.1", "MySQL host")
	port     = flag.Int("port", 3306, "MySQL port")
	username = flag.String("user", "root", "User name")
	password = flag.String("password", "", "Password")
	schema   = flag.String("schema", "test", "Schema")
)

const (
	WelcomeInfo = "ExecutableChecker: Check if SQL can be successfully executed by TiDB\n" +
		"Copyright 2018 PingCAP, Inc.\n\n" +
		"You can switch modes using the `SETMOD` command.\n" +
		"Auto mode: The program will automatically synchronize the dependent table structure from MYSQL " +
		"and delete the conflict table\n" +
		"Query mode: The program will ask you before synchronizing the dependent table structure from MYSQL\n" +
		"Manual mode: This program does not perform anything other than executing the input SQL.\n" +
		"SETMOD Usage: SETMOD <MODCODE>; MODCODE = [0(Auto), 1(Query), 2(Manual)]\n"
)

func main() {
	initialise()
	printWelcome()
	mainLoop()
	//handler("ALTER TABLE table_name MODIFY column_1 int(11) NOT NULL;")
	destroy()
}

func initialise() {
	var err error
	reader = bufio.NewReader(os.Stdin)
	executableChecker, err = ddl_checker.NewExecutableChecker()
	if err != nil {
		fmt.Printf("[DDLChecker]Init failed,Can't create ExecutableChecker: %s\n", err.Error())
		os.Exit(1)
	}
	executableChecker.Execute("use test;")
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		*username, *password, *host, *port, *schema))
	if err != nil {
		fmt.Printf("[DDLChecker]Init failed,Can't open mysql database: %s\n", err.Error())
		os.Exit(1)
	}

}

func destroy() {
	executableChecker.Close()
	db.Close()
}

func mainLoop() {
	var input string
	var err error
	for cont := true; cont; cont = handler(input) {
		fmt.Printf("[%s] > ", modeName())
		input, err = reader.ReadString(';')
		if err != nil {
			fmt.Printf("[DDLChecker]Read Stdin Error: %s\n", err.Error())
			os.Exit(1)
		}
	}
}
func handler(input string) bool {
	// cmd exit
	lowerTrimInput := strings.ToLower(strings.TrimSpace(input[:len(input)-1]))
	if lowerTrimInput == "exit" {
		return false
	}
	// cmd setmod
	if strings.HasPrefix(lowerTrimInput, "setmod") {
		modCodeTmp, err := strconv.Atoi(strings.TrimSpace(lowerTrimInput[6:]))
		if err != nil || modCodeTmp > 2 || modCodeTmp < 0 {
			fmt.Printf("[DDLChecker]SETMOD Usage: SETMOD <MODCODE>; MODCODE = [0(Auto), 1(Query), 2(Manual)]\n")
		} else {
			modCode = modCodeTmp
		}
		return true
	}
	if modCode != 2 {
		// auto and query mod
		stmt, err := executableChecker.Parse(input)
		if err != nil {
			fmt.Printf("[DDLChecker]SQL Parse Error: %s\n", err.Error())
			return true
		}
		neededTable := ddl_checker.GetTableNeededExist(stmt)
		nonNeededTable := ddl_checker.GetTableNeededNonExist(stmt)
		// query mod
		if modCode == 1 && !queryAutoSync(neededTable, nonNeededTable) {
			goto EXECUTE
		}
		syncTablesFromMysql(neededTable)
		dropTables(nonNeededTable)
	}
EXECUTE:
	err := executableChecker.Execute(input)
	if err == nil {
		fmt.Println("[DDLChecker] SQL execution succeeded")
	} else {
		fmt.Println("[DDLChecker] SQL execution failed:", err.Error())
	}
	return true
}

func syncTablesFromMysql(tableNames []string) {
	for _, tableName := range tableNames {
		fmt.Println("[DDLChecker] Syncing Table", tableName)
		isExist := executableChecker.IsTableExist(tableName)
		if isExist {
			fmt.Println("[DDLChecker] Table", tableName, "is exist,Skip")
			continue
		}
		rows, err := db.Query("show create table " + tableName)
		if err != nil {
			fmt.Println("[DDLChecker] Sync Error:", err.Error())
			continue
		}
		var table string
		var createTableDDL string
		if rows.Next() {
			err = rows.Scan(&table, &createTableDDL)
			if err != nil {
				fmt.Println("[DDLChecker] SQL Execute Error:", err.Error())
				continue
			}
		} else {
			fmt.Println("[DDLChecker] Can't get", tableName, "DDL")
			continue
		}
		err = executableChecker.Execute(createTableDDL)
		if err != nil {
			fmt.Println("[DDLChecker] Create table failure:", err.Error())
			continue
		}
	}
}

func dropTables(tableNames []string) {
	for _, tableName := range tableNames {
		fmt.Println("[DDLChecker] Dropping Table", tableName)
		err := executableChecker.Execute(fmt.Sprintf("drop table if exists %s", tableName))
		if err != nil {
			fmt.Println("[DDLChecker] DROP TABLE", tableName, "Error:", err.Error())
		}
	}
}

func queryAutoSync(neededTable []string, nonNeededTable []string) bool {
	for {
		fmt.Printf("[DDLChecker] Do you want to synchronize table %v from MySQL "+
			"and drop table %v in ExecutableChecker?(Y/N)", neededTable, nonNeededTable)
		QUERY:
		result, err := reader.ReadString('\n')
		if err != nil {
			return false
		}
		switch strings.ToLower(strings.TrimSpace(result)) {
		case "y":
			return true
		case "n":
			return false
		case "":
			goto QUERY
		}
	}
}

func printWelcome() {
	fmt.Println(WelcomeInfo)
}

func modeName() string {
	switch modCode {
	case 0:
		return "Auto"
	case 1:
		return "Query"
	case 2:
		return "Manual"
	default:
		return "Unknown"
	}
}

