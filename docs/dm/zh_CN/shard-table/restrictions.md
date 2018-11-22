sharding DDL 使用限制
===

- 在一个 sharding group 内的所有表都必须以相同的顺序执行相同的 DDL
    - 暂时不支持 sharding DDL 乱序交叉执行
    - 只要有任意一个表的 DDL 未执行，同步过程都会被挂起
- 一个 sharding group 必须等待一个 DDL 完全执行完毕后，才能执行下一个 DDL
- sharding group 数据同步任务不支持 `DROP DATABASE / TABLE`
- sharding group 支持 `RENAME TABLE`，但是有下面的限制
    - 只支持 `RENAME TABLE` 到一个不存在的表
    - 一个 `RENAME TABLE` 语句只能有一个 `RENAME` 操作 （online DDL 有别的方案支持）
- 各表任务开始时的同步位置点应当在需要同步的所有 sharding DDL 之前（或之后）
    - 如果开始同步位置点时有部分 sharding DDL 已经执行，则该 sharding DDL 将永远不能同步匹配成功
- 如果需要变更 `router-rules`，需要先等所有 sharding DDL 同步完成
    - 当 sharding DDL 在同步过程中时，使用 dmctl 尝试变更 `router-rules` 会报错
- 如果需要 `CREATE` 新表加入到已有的 sharding group 中，需要保持和最新更改的表结构一致
    - 比如原 table_a, table_b 初始时有 (a, b) 两列，sharding DDL 后有 (a, b, c) 三列，则同步完成后新 `CREATE` 的表应当有 (a, b, c) 三列
- sharding DDL lock 等待同步时，如果重启了 DM-master，会由于 lock 信息丢失而造成同步过程 block
