列值映射
===

### 索引
- [功能介绍](#功能介绍)
- [参数配置](#参数配置)
- [参数解释](#参数解释)
  - [partition id 表达式](#partition-id)
- [使用示例](#使用示例)

### 功能介绍

column mapping 提供对表的列值进行修改的功能。可以根据不同的表达式对表的指定列做不同的修改操作，目前只支持 DM 提供的内置表达式。

注意：
- 不支持修改 column 的类型和表结构
- 不支持对同一个表设置多个不同的列值转换规则

### 参数配置

```yaml
column-mappings:
  rule-1:
​    schema-pattern: "test_*"
​    table-pattern: "t_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["1", "test_", "t_"]
  rule-2:
​    schema-pattern: "test_*"
​    table-pattern: "t_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["2", "test_", "t_"]
```

### 参数解释

- [`schema-pattern` / `table-pattern`](./table-selector.md): 对匹配上该规则的上游 MySQL/MariaDB 实例的表按照指定 `expression` 进行列值修改操作
- `source-column`，`target-column`：对 `source-column` 列的值按照指定 `expression` 进行修改，将修改后的值赋值给 `target-column`
- `expression`: 对数据进行转换的表达式，目前只支持下面的内置计算表达式

#### partition id

`partition id` 目的是为了解决分库分表合并同步的自增主键的冲突

##### 使用注意
- 只支持类型为 bigint 的自增主键
- 库名的组成必须为 `schema 前缀 + 数字（既 schema ID）`，例如: 支持 `s_1`, 不支持 `s_a`
- 表名的组成必须为 `table 前缀 + 数字（既 table ID）`
- 对分库分表的规模支持限制如下
  - 支持最多 16 个 MySQL/MariaDB 实例（0 <= instance ID <= 15）
  - 每个实例支持最多 128 个 schema（0 <= schema ID  <= 127）
  - 每个实例的每个 schema 256 个 table（0 <= table ID <= 255）
  - 自增主键 ID 范围 (0 <= ID <= 17592186044415)
  - {instance ID、schema ID、table ID} 组合需要保持唯一
- 目前该功能是定制功能，如果需要调整请联系相关开发人员进行调整

##### arguments 设置

用户需要在 arguments 里面顺序设置三个参数
- instance_id: 客户指定的上游分库分表的 MySQL/MariaDB instance ID（0 <= instance ID <= 15）
- schema 前缀: 用来解析库名获取 `schema ID`
- table 前缀： 用来解释表名获取 `table ID`


##### 表达式规则
`partition id` 会将 arguments 里面的数值填充自增主键 ID 的首部比特位， 计算出来一个 int64 (既 MySQL bigint) 类型的值， 具体规则如下：

int64 比特表示 `[1:1 bit] [2:4 bits] [3：7 bits] [4:8 bits] [5: 44 bits]` 
- 1： 符号位，保留
- 2： instance ID ，默认 4 bits
- 3： schema ID，默认 7 bits
- 4： table ID，默认 8 bits
- 5： 自增主键 ID，默认 44 bits

### 使用示例

下面例子都假设存在分库分表场景 - 将上游两个 MySQL 实例 `test_{1,2,3...}`.`t_{1,2,3...}` 同步到下游 TiDB 的 `test`.`t`，并且这些表都有自增主键

需要设置下面两个规则
```yaml
column-mappings:
  rule-1:
​    schema-pattern: "test_*"
​    table-pattern: "t_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["1", "test_", "t_"]
  rule-2:
​    schema-pattern: "test_*"
​    table-pattern: "t_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["2", "test_", "t_"]
```

- MySQL instance 1 的表 `test_1`.`t_1` 的 `ID = 1` 的列经过转换后 ID = 1 变为 `1 << (64-1-4) | 1 << (64-1-4-7) | 1 << 44 | 1 = 580981944116838401`
- MySQL instance 2 的表 `test_1`.`t_2` 的 `ID = 1` 的行经过转换后 ID = 2 变为 `2 << (64-1-4) | 1 << (64-1-4-7) | 2 << 44 | 2 = 1157460288606306306`
