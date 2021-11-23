package config

import (
	"os"

	"github.com/pingcap/errors"
)

func ExportTemplateConfig(configType string) error {
	switch configType {
	case "dm", "DM", "Dm", "dM":
		return exportTemplateConfigDM()
	case "norm", "normal", "Norm", "Normal":
		return exportTemplateConfigNorm()
	default:
		return errors.Errorf("Error: unexpect template name: %s\n-T dm: export a dm config\n-T norm: export a normal config\n", configType)
	}
}

func exportTemplateConfigDM() error {
	file, err := os.Create("./config_dm_template.toml")
	if err != nil {
		return errors.Annotatef(err, "Error: fail to create a dm template config file, filename is './config_dm_template.toml'.\nError info: %s\n", err.Error())
	}
	_, err = file.WriteString(`# Diff Configuration.

######################### Global config #########################

check-thread-count = 4

export-fix-sql = true

check-struct-only = false

dm-addr = "http://127.0.0.1:8261"

dm-task = "test"

######################### Task config #########################
[task]
	output-dir = "./output"

	target-check-tables = ["hb_test.*"]
`)
	if err != nil {
		return errors.Annotatef(err, "Error: fail to write the template config into the file\nError info: %s\n", err.Error())
	}

	file.Close()
	return nil
}

func exportTemplateConfigNorm() error {
	file, err := os.Create("./config_template.toml")
	if err != nil {
		return errors.Annotatef(err, "Error: fail to create a template config file, filename is './config_template.toml'.\nError info: %s\n", err.Error())
	}

	_, err = file.WriteString(`# Diff Configuration.

######################### Global config #########################

check-thread-count = 4

export-fix-sql = true

check-struct-only = false


######################### Datasource config #########################
[data-sources]
[data-sources.mysql1]
	host = "127.0.0.1"
	port = 3306
	user = "root"
	password = ""

	route-rules = ["rule1", "rule2"]

[data-sources.tidb0]
	host = "127.0.0.1"
	port = 4000
	user = "root"
	password = ""

	# snapshot = "386902609362944000"

########################### Routes ###########################
[routes]
[routes.rule1]
schema-pattern = "test_*"
table-pattern = "t_*"
target-schema = "test"
target-table = "t"

[routes.rule2]
schema-pattern = "test2_*"
table-pattern = "t2_*"
target-schema = "test2"
target-table = "t2"

######################### Task config #########################
[task]
	output-dir = "./output"

	source-instances = ["mysql1"]

	target-instance = "tidb0"

	target-check-tables = ["schema*.table*", "!c.*", "test2.t2"]

	target-configs = ["config1"]

######################### Table config #########################
[table-configs.config1]
target-tables = ["schema*.test*", "test2.t2"]
range = "age > 10 AND age < 20"
index-fields = ["col1","col2"]
ignore-columns = ["",""]
chunk-size = 0
collation = ""
`)
	if err != nil {
		return errors.Annotatef(err, "Error: fail to write the template config into the file\nError info: %s\n", err.Error())
	}

	file.Close()
	return nil
}
