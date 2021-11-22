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
	
	# 检查数据的线程数量，上下游数据库的连接数会略大于该值
	check-thread-count = 4
	
	# 如果开启，若表存在不一致，则输出用于修复的 SQL 语句。
	export-fix-sql = true
	
	# 只对比表结构而不对比数据
	check-struct-only = false
	
	
	######################### Datasource config #########################
	[data-sources]
	[data-sources.mysql1] # mysql1 是该数据库实例唯一标识的自定义 id，用于下面 task.source-instances/task.target-instance 中
		host = "127.0.0.1"
		port = 3306
		user = "root"
		password = ""
	
		#（可选）使用映射规则来匹配上游多个分表，其中 rule1 和 rule2 在下面 Routes 配置栏中定义
		route-rules = ["rule1", "rule2"]
	
	[data-sources.tidb0]
		host = "127.0.0.1"
		port = 4000
		user = "root"
		password = ""
		#（可选）使用 TiDB 的 snapshot 功能，如果开启的话会使用历史数据进行对比
		# snapshot = "386902609362944000"
	
	########################### Routes ###########################
	# 如果需要对比大量的不同库名或者表名的表的数据，或者用于校验上游多个分表与下游总表的数据，可以通过 table-rule 来设置映射关系
	# 可以只配置 schema 或者 table 的映射关系，也可以都配置
	[routes]
	[routes.rule1] # rule1 是该配置的唯一标识的自定义 id，用于上面 data-sources.route-rules 中
	schema-pattern = "test_*"      # 匹配数据源的库名，支持通配符 "*" 和 "?"
	table-pattern = "t_*"          # 匹配数据源的表名，支持通配符 "*" 和 "?"
	target-schema = "test"         # 目标库名
	target-table = "t" # 目标表名
	
	[routes.rule2]
	schema-pattern = "test2_*"      # 匹配数据源的库名，支持通配符 "*" 和 "?"
	table-pattern = "t2_*"          # 匹配数据源的表名，支持通配符 "*" 和 "?"
	target-schema = "test2"         # 目标库名
	target-table = "t2" # 目标表名
	
	######################### Task config #########################
	# 配置需要对比的*目标数据库*中的表
	[task]
		# output-dir 会保存如下信息
		# 1 sql: 检查出错误后生成的修复 SQL 文件，并且一个 chunk 对应一个文件
		# 2 log: sync-diff.log 保存日志信息
		# 3 summary: summary.txt 保存总结
		# 4 checkpoint: a dir 保存断点续传信息
		output-dir = "./output"
	
		# 上游数据库，内容是 data-sources 声明的唯一标识 id
		source-instances = ["mysql1"]
	
		# 下游数据库，内容是 data-sources 声明的唯一标识 id
		target-instance = "tidb0"
	
		# 需要比对的下游数据库的表，每个表需要包含数据库名和表名，两者由 . 隔开
		# 使用 ? 来匹配任意一个字符；使用 * 来匹配任意；详细匹配规则参考 golang regexp pkg: https://github.com/google/re2/wiki/Syntax
		target-check-tables = ["schema*.table*", "!c.*", "test2.t2"]
	
		#（可选）对部分表的额外配置，其中 config1 在下面 Table config 配置栏中定义
		target-configs= ["config1"]
	
	######################### Table config #########################
	# 对部分表进行特殊的配置，配置的表必须包含在 task.target-check-tables 中
	[table-configs.config1] # config1 是该配置的唯一标识自定义 id，用于上面 task.target-configs 中
	# 目标表名称，可以使用正则来匹配多个表，但不允许存在一个表同时被多个特殊配置匹配。
	target-tables = ["schema*.test*", "test2.t2"]
	#（可选）指定检查的数据的范围，需要符合 sql 中 where 条件的语法
	range = "age > 10 AND age < 20"
	#（可选）指定用于划分 chunk 的列，如果不配置该项，sync-diff-inspector 会选取一些合适的列（主键／唯一键／索引）
	index-fields = ["col1","col2"]
	#（可选）忽略某些列的检查，例如 sync-diff-inspector 目前还不支持的一些类型（json，bit，blob 等），
	# 或者是浮点类型数据在 TiDB 和 MySQL 中的表现可能存在差异，可以使用 ignore-columns 忽略检查这些列
	ignore-columns = ["",""]
	#（可选）指定划分该表的 chunk 的大小，若不指定可以删去或者将其配置为 0。
	chunk-size = 0
	#（可选）指定该表的 collation，若不指定可以删去或者将其配置为空字符串。
	collation = ""
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
	
	# 检查数据的线程数量，上下游数据库的连接数会略大于该值
	check-thread-count = 4
	
	# 如果开启，若表存在不一致，则输出用于修复的 SQL 语句
	export-fix-sql = true
	
	# 只对比表结构而不对比数据
	check-struct-only = false
	
	# dm-master 的地址, 格式为 "http://127.0.0.1:8261"
	dm-addr = "http://127.0.0.1:8261"
	
	# 指定 DM 的 task-name
	dm-task = "test"
	
	######################### Task config #########################
	[task]
		output-dir = "./output"
	
		# 需要比对的下游数据库的表，每个表需要包含数据库名和表名，两者由 . 隔开
		target-check-tables = ["hb_test.*"]
	`)
	if err != nil {
		return errors.Annotatef(err, "Error: fail to write the template config into the file\nError info: %s\n", err.Error())
	}

	file.Close()
	return nil
}
