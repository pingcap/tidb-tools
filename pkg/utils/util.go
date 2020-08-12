package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
)

// SliceToMap converts slice to map
func SliceToMap(slice []string) map[string]interface{} {
	sMap := make(map[string]interface{})
	for _, str := range slice {
		sMap[str] = struct{}{}
	}
	return sMap
}

// StringsToInterfaces converts string slice to interface slice
func StringsToInterfaces(strs []string) []interface{} {
	is := make([]interface{}, 0, len(strs))
	for _, str := range strs {
		is = append(is, str)
	}

	return is
}

// GetJSON fetches a page and parses it as JSON. The parsed result will be
// stored into the `v`. The variable `v` must be a pointer to a type that can be
// unmarshalled from JSON.
//
// Example:
//
//	client := &http.Client{}
//	var resp struct { IP string }
//	if err := util.GetJSON(client, "http://api.ipify.org/?format=json", &resp); err != nil {
//		return errors.Trace(err)
//	}
//	fmt.Println(resp.IP)
func GetJSON(client *http.Client, url string, v interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Errorf("get %s http status code != 200, message %s", url, string(body))
	}

	return errors.Trace(json.NewDecoder(resp.Body).Decode(v))
}

// GetGlobalVariable gets server's global variable
func GetGlobalVariable(db *sql.DB, variable string) (value string, err error) {
	query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", variable)
	rows, err := db.Query(query)

	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/

	for rows.Next() {
		err = rows.Scan(&variable, &value)
		if err != nil {
			return "", errors.Trace(err)
		}
	}

	if rows.Err() != nil {
		return "", errors.Trace(err)
	}

	return value, nil
}

// GetSQLMode returns sql_mode.
func GetSQLMode(db *sql.DB) (tmysql.SQLMode, error) {
	sqlMode, err := GetGlobalVariable(db, "sql_mode")
	if err != nil {
		return tmysql.ModeNone, err
	}

	mode, err := tmysql.GetSQLMode(sqlMode)
	return mode, errors.Trace(err)
}

// HasAnsiQuotesMode checks whether database has `ANSI_QUOTES` set
func HasAnsiQuotesMode(db *sql.DB) (bool, error) {
	mode, err := GetSQLMode(db)
	if err != nil {
		return false, err
	}
	return mode.HasANSIQuotesMode(), nil
}
