阿里云 RDS 支持限制
===

### 内容索引

- [implict_primary_key 造成同步中断](#implict_primary_key-造成同步中断)
- [缺少 SUPER privilege(s) 权限 dump 失败](#缺少-super-privileges-权限-dump-失败)

> 注意：目前 DM 未针对阿里云 RDS 进行完备的测试，除以下已列出的功能在支持上受限外，可能还存在其他功能无法完整支持，如需在生产环境中使用 DM，请先根据实际业务场景进行完备的测试。

### implict_primary_key 造成同步中断

当打开阿里云 RDS 特有的 `implict_primary_key` 参数后，如果用户创建的表中没有指定主键或唯一键，RDS 会自动为表创建隐式主键。

当使用 DM 从包含隐式主键的表增量同步数据到下游 TiDB 时，会由于 binlog event 中包含隐式主键数据而造成与下游表结构的不匹配，导致数据同步中断。

此外 TiDB 不支持 RDS 的 `implict_primary_key`，并且 DM 对不存在主键或者唯一索引的表的同步不保证正确性，建议对所有的表加上主键。

#### 解决办法

1. 对于将要创建的表，在创建表时显式地指定主键
2. 对于已经存在且包含隐式主键的表，在关闭 `implict_primary_key` 参数后，显式地为表增加主键

### 缺少 SUPER 权限造成 dump 失败

为获取一致性的 dumped data 和 binlog metadata，dump 时需要相关权限进行 `LOCK TABLES` 等操作。

但阿里云 RDS user 可能无法配置这样的权限，而造成在 dump 阶段报类似如下的错误

```bash
Couldn't acquire LOCK TABLES FOR BACKUP, snapshots will not be consistent: Access denied; you need (at least one of) the SUPER privilege(s) for this operation
```

#### 解决办法

1. 确保需要 dump 后进行增量同步的表**存在主键或者唯一索引**
2. 在上游 RDS 执行 `SHOW MASTER STATUS` 获取当前的 binlog position 信息
3. 启动一个 `task-mode: full` 的全量数据同步任务，从上游 RDS 进行全量数据的导出与导入
    - 需要在同步任务配置文件中的 `mydumper` 配置项下的 `extra-args` 配置项增加 `--no-locks` 参数，如：
    ```yaml
    mydumpers:
      global:
        extra-args: "-B test -T t1,t2 --no-locks"
    ```
4. 在全量任务执行完成后，使用 Step.2 时获取到的 binlog position 信息启动一个 `task-mode: incremental` 的增量数据同步任务进行后续的数据同步
    - 在 `mysql-instances` 中为指定 instance 的 `meta` 设置 binlog position，如：
    ```yaml
    mysql-instances:
      -
        source-id: "instance118-4306"
        meta:
          binlog-name: mysql-bin.000123
          binlog-pos: 45678
    ```
