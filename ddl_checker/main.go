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
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"unicode"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	checker "github.com/pingcap/tidb-tools/pkg/ddl-checker"
)

var (
	mode              = auto
	executableChecker *checker.ExecutableChecker
	ddlSyncer         *checker.DDLSyncer
	reader            *bufio.Reader
	tidbContext       = context.Background()

	host     = flag.String("host", "127.0.0.1", "MySQL host")
	port     = flag.Int("port", 3306, "MySQL port")
	username = flag.String("user", "root", "User name")
	password = flag.String("password", "", "Password")
	schema   = flag.String("schema", "", "Schema")
)

const (
	welcomeInfo = "ExecutableChecker: Check if SQL can be successfully executed by TiDB\n" +
		"Copyright 2018 PingCAP, Inc.\n\n" +
		"You can switch modes using the `SETMOD` command.\n" +
		"Auto mode: The program will automatically synchronize the dependent table structure from MySQL " +
		"and delete the conflict table\n" +
		"Prompt mode: The program will ask you before synchronizing the dependent table structure from MYSQL\n" +
		"Offline mode: This program doesn't need to connect to MySQL, and doesn't perform anything other than executing the input SQL.\n\n" +
		setmodUsage + "\n"

	setmodUsage = "SETMOD usage: SETMOD <MODCODE>; MODCODE = [\"Auto\", \"Prompt\", \"Offline\"] (case insensitive).\n"

	auto    = "auto"
	prompt  = "prompt"
	offline = "offline"
)

func main() {
	fmt.Print(welcomeInfo)
	initialise()
	mainLoop()
	destroy()
}

func initialise() {
	flag.Parse()
	var err error
	reader = bufio.NewReader(os.Stdin)
	executableChecker, err = checker.NewExecutableChecker()
	if err != nil {
		fmt.Printf("[DDLChecker] Init failed, can't create ExecutableChecker: %s\n", err.Error())
		os.Exit(1)
	}
	executableChecker.Execute(tidbContext, "use test;")
	dbInfo := &dbutil.DBConfig{
		User:     *username,
		Password: *password,
		Host:     *host,
		Port:     *port,
		Schema:   *schema,
	}
	ddlSyncer, err = checker.NewDDLSyncer(dbInfo, executableChecker)
	if err != nil {
		fmt.Printf("[DDLChecker] Init failed, can't open mysql database: %s\n", err.Error())
		os.Exit(1)
	}
}

func destroy() {
	executableChecker.Close()
	ddlSyncer.Close()
}

func mainLoop() {
	var input string
	var err error
	for isContinue := true; isContinue; isContinue = handler(input) {
		fmt.Printf("[%s] > ", strings.ToTitle(mode))
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
		x := strings.TrimSpace(lowerTrimInput[6:])
		switch x {
		case auto, prompt, offline:
			mode = x
		default:
			fmt.Print(setmodUsage)
		}
		return true
	}
	stmt, err1 := executableChecker.Parse(input)
	if err1 != nil {
		fmt.Println("[DDLChecker] SQL parse error: ", err1.Error())
		return true
	}
	if !checker.IsDDL(stmt) {
		fmt.Println("[DDLChecker] Warning: The input SQL isn't a DDL")
	}
	if mode != offline {
		// auto and query mod
		neededTables, _ := checker.GetTablesNeededExist(stmt)
		nonNeededTables, err := checker.GetTablesNeededNonExist(stmt)
		// skip when stmt isn't a DDLNode
		if err == nil && (mode == auto || (mode == prompt && promptAutoSync(neededTables, nonNeededTables))) {
			err := syncTablesFromMysql(neededTables)
			if err != nil {
				return true
			}
			err = dropTables(nonNeededTables)
			if err != nil {
				return true
			}
		}
	}
	err := executableChecker.Execute(tidbContext, input)
	if err == nil {
		fmt.Println("[DDLChecker] SQL execution succeeded")
	} else {
		fmt.Println("[DDLChecker] SQL execution failed:", err.Error())
	}
	return true
}

func syncTablesFromMysql(tableNames []string) error {
	for _, tableName := range tableNames {
		fmt.Println("[DDLChecker] Syncing Table", tableName)
		err := ddlSyncer.SyncTable(tidbContext, *schema, tableName)
		if err != nil {
			fmt.Println("[DDLChecker] Sync table failure:", err.Error())
			return errors.Trace(err)
		}
	}
	return nil
}

func dropTables(tableNames []string) error {
	for _, tableName := range tableNames {
		fmt.Println("[DDLChecker] Dropping table", tableName)
		err := executableChecker.DropTable(tidbContext, tableName)
		if err != nil {
			fmt.Println("[DDLChecker] Drop table", tableName, "Error:", err.Error())
			return errors.Trace(err)
		}
	}
	return nil
}

func promptAutoSync(neededTable []string, nonNeededTable []string) bool {
	return promptYorN("[DDLChecker] Do you want to synchronize table %v from MySQL "+
		"and drop table %v in DDLChecker?(Y/N)", neededTable, nonNeededTable)
}

func promptYorN(format string, a ...interface{}) bool {
	for {
		fmt.Printf(format, a...)
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
