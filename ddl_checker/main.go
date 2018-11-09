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
	"strings"
	"unicode"
)

var (
	modCode           uint8
	executableChecker *ddl_checker.ExecutableChecker
	reader            *bufio.Reader
	db                *sql.DB

	host     = flag.String("host", "127.0.0.1", "MySQL host")
	port     = flag.Int("port", 3306, "MySQL port")
	username = flag.String("user", "root", "User name")
	password = flag.String("password", "", "Password")
	schema   = flag.String("schema", "", "Schema")
)

const (
	WelcomeInfo = "ExecutableChecker: Check if SQL can be successfully executed by TiDB\n" +
		"Copyright 2018 PingCAP, Inc.\n\n" +
		"You can switch modes using the `SETMOD` command.\n" +
		"Auto mode: The program will automatically synchronize the dependent table structure from MySQL " +
		"and delete the conflict table\n" +
		"Prompt mode: The program will ask you before synchronizing the dependent table structure from MYSQL\n" +
		"Offline mode: This program doesn't need to connect to MySQL, and doesn't perform anything other than executing the input SQL.\n\n" +
		"SETMOD usage: SETMOD <MODCODE>; MODCODE = [\"Auto\", \"Prompt\", \"Offline\"] (case insensitive).\n\n"
)

func main() {
	initialise()
	printWelcome()
	mainLoop()
	destroy()
}

func initialise() {
	flag.Parse()
	var err error
	reader = bufio.NewReader(os.Stdin)
	executableChecker, err = ddl_checker.NewExecutableChecker()
	if err != nil {
		fmt.Printf("[DDLChecker] Init failed, can't create ExecutableChecker: %s\n", err.Error())
		os.Exit(1)
	}
	executableChecker.Execute("use test;")
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		*username, *password, *host, *port, *schema))
	if err != nil {
		fmt.Printf("[DDLChecker] Init failed, can't open mysql database: %s\n", err.Error())
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
			fmt.Printf("[DDLChecker] Read stdin error: %s\n", err.Error())
			os.Exit(1)
		}
	}
}
func handler(input string) bool {
	lowerTrimInput := strings.ToLower(strings.TrimFunc(input, func(r rune) bool {
		return unicode.IsSpace(r) || r == ';'
	}))
	// cmd exit
	if lowerTrimInput == "exit" {
		return false
	}
	// cmd setmod
	if strings.HasPrefix(lowerTrimInput, "setmod") {
		switch strings.TrimSpace(lowerTrimInput[6:]) {
		case "auto":
			modCode = 0
		case "prompt":
			modCode = 1
		case "offline":
			modCode = 2
		default:
			fmt.Println("SETMOD usage: SETMOD <MODCODE>; MODCODE = [\"Auto\", \"Prompt\", \"Offline\"] (case insensitive).")
		}
		return true
	}
	if modCode != 2 {
		// auto and query mod
		stmt, err := executableChecker.Parse(input)
		if err != nil {
			fmt.Printf("[DDLChecker] SQL parse error: %s\n", err.Error())
			return true
		}
		neededTables, _ := ddl_checker.GetTablesNeededExist(stmt)
		nonNeededTables, err := ddl_checker.GetTablesNeededNonExist(stmt)
		// skip when stmt isn't a DDLNode
		if err == nil && (modCode == 0 || (modCode == 1 && promptAutoSync(neededTables, nonNeededTables))) {
			syncTablesFromMysql(neededTables)
			dropTables(nonNeededTables)
		}
	}
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
		row := db.QueryRow("show create table `" + tableName + "`")
		var table string
		var createTableDDL string
		err := row.Scan(&table, &createTableDDL)
		if err != nil {
			fmt.Println("[DDLChecker] SQL Execute Error:", err.Error())
			continue
		}
		isExist := executableChecker.IsTableExist(tableName)
		if isExist && promptYorN(fmt.Sprintf("[DDLChecker] Table %s exist, "+
			"do you want to override it to be synchronized from MySQL?(Y/N)", tableName)) {
			err := executableChecker.Execute(fmt.Sprintf("drop table if exists `%s`", tableName))
			if err != nil {
				fmt.Println("[DDLChecker] DROP TABLE", tableName, "Error:", err.Error())
			}
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
		fmt.Println("[DDLChecker] Dropping table", tableName)
		err := executableChecker.Execute(fmt.Sprintf("drop table if exists `%s`", tableName))
		if err != nil {
			fmt.Println("[DDLChecker] DROP TABLE", tableName, "Error:", err.Error())
		}
	}
}

func promptAutoSync(neededTable []string, nonNeededTable []string) bool {
	return promptYorN(fmt.Sprintf("[DDLChecker] Do you want to synchronize table %v from MySQL "+
		"and drop table %v in DDLChecker?(Y/N)", neededTable, nonNeededTable))
}

func promptYorN(info string) bool {
	for {
		fmt.Print(info)
	innerLoop:
		for {
			result, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("[DDLChecker] Read stdin error: %s\n", err.Error())
				return false
			}
			switch strings.ToLower(strings.TrimSpace(result)) {
			case "y", "yes":
				return true
			case "n", "no":
				return false
			case "":
				continue innerLoop
			default:
				break innerLoop
			}
		}
	}
}

func printWelcome() {
	fmt.Print(WelcomeInfo)
}

func modeName() string {
	switch modCode {
	case 0:
		return "Auto"
	case 1:
		return "Prompt"
	case 2:
		return "Offline"
	default:
		return "Unknown"
	}
}
