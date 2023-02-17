# 场景说明
Hologres是兼容PostgreSQL协议的一站式实时数仓引擎，支持海量数据实时写入、实时更新、实时分析，既支持PB级数据多维分析（OLAP）与即席分析（Ad Hoc），又支持高并发低延迟的在线数据服务（Serving）。
<br />本程序为为Hologres在数据写入、数据更新、点查场景的性能测试工具：

- 数据写入场景：主要用于测试引擎针对行存、列存、行列共存表进行数据写入的性能。
- 数据更新场景：主要用于测试引擎针对行存、列存、行列共存表，在有主键的情况下进行数据更新的性能。
- 点查场景：主要用于测试引擎针对行存、行列共存表进行主键过滤的点查性能。
- 前缀扫描场景：主要用于测试引擎对行存，行列共存表进行前缀主键扫描的查询性能。

# 使用说明
## 数据写入场景
### 原理说明
写入模式说明：数据写入可以分为Fixed copy和Insert两种模式。

- Fixed copy模式：通过COPY语句进行数据写入，同时通过Fixed Plan优化SQL执行，详情请参见[Fixed Plan加速SQL执行](https://help.aliyun.com/document_detail/408830.html)。
- Insert模式：通过INSERT语句进行数据写入，同时通过Fixed Plan优化SQL执行。

特殊配制说明：

- 为便于兼容各测试场景、高效推进测试与验证，测试工具会在配置的表列数基础上，额外增加以下列：
   - 主键列id：作为表的主键和distribution_key，在测试过程中从1开始逐条递增。
   - 时间列ts：作为表的segment_key，在测试过程中写入当前时间。

数据写入原理：

- 在测试过程中，测试工具会将主键id从1开始逐条递增写入数据。针对时间列，写入当前时间。针对配置的其他TEXT列，写入目标长度的字符串+主键id。在达到目标时间或目标行数后停止写入，计算测试结果。
### 操作步骤

- 首先创建写入配置文件
```
-- 连接配置
holoClient.jdbcUrl=jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>
holoClient.username=<AccessKey_ID>
holoClient.password=<AccessKey_Secret>
holoClient.writeThreadSize=100

-- 写入配置
put.threadSize=8
put.testByTime=false
put.rowNumber=200000000
put.testTime=600000

-- 表配置
put.tableName=kv_test
put.columnCount=20
put.columnSize=20
put.orientation=row

-- 其他配置
put.createTableBeforeRun=true
put.deleteTableAfterDone=false
put.vacuumTableBeforeRun=false
```

- 配置参数说明：

| 模块 | 参数 | 默认值 | 描述 | 备注 |
| --- | --- | --- | --- | --- |
| 连接配置 | jdbcUrl | 空 | Hologres的JDBC连接串，格式为jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>。 | ENDPOINT需要填写Hologres实例的VPC网络地址。 |
|  | username | 空 | 当前阿里云账号的AccessKey ID。 | 您可以登录[AccessKey 管理](https://ram.console.aliyun.com/manage/ak?spm=5176.2020520207.nav-right.dak.538b4c12VYbuIb)，获取AccessKey ID。 |
|  | password | 空 |当前阿里云账号的AccessKey Secret。 | 您可以登录[AccessKey 管理](https://ram.console.aliyun.com/manage/ak?spm=5176.2020520207.nav-right.dak.538b4c12VYbuIb)，获取AccessKey Secret。 |
|  | writeThreadSize | 1 | 每个holo-client启动的写入线程数。每个holo-client写入线程会占用1个连接。仅INSERT模式生效。 | Holo Client详情请参见[通过Holo Client读写数据](https://help.aliyun.com/document_detail/201027.html)。 |
| 写入配置 | threadSize | 10 | 线程数。每个线程会创建一个holo-client。<br /> | <br />- FIXED_COPY模式下，每个线程会占用一个连接，总连接数即为线程数。<br />- INSERT模式下，总连接数为线程数threadSize*单线程连接数writeThreadSize。<br />针对本文测试的64核实例规格，在FIXED_COPY模式下推荐设置线程数为8。您可以根据实例规格适当调整该参数。 |
|  | testByTime | true | 按规定时间进行测试或按规定行数进行测试。 | 参数取值如下：<br />- true：表示按规定时间进行测试。<br />- false：表示按规定行数进行测试。<br /> |
|  | rowNumber | 1000000 | 测试的目标行数，testByTime为false时生效。 |  |
|  | testTime | 600000 | 测试的目标时间，单位为毫秒，testByTime为true时生效。 |  |
| 表配置 | tableName | holo_perf | 测试的目标表名。 |  |
|  | columnCount | 100 | 表的列数。每列的数据类型均为TEXT。 | 主键列和ts时间列不包含在计数内 |
|  | columnSize | 10 | 表每列的字符长度(byte)。 |  |
|  | orientation | column | 表的存储类型。 | 参数取值如下：<br />- row：行存表<br />- column：列存表<br />- row,column：行列共存表<br /> |
|  | hasPk | true | 是否有主键 ||
| 其他配置 | createTableBeforeRun | true | 测试开始前是否创建表。 | 参数取值如下：<br />- true：表示建表。<br />- false：表示不建表。<br />如果设为true，测试工具默认会先执行删除同名表、再创建表的操作，请注意目标表名不与实例中其他表名相同。 |
|  | deleteTableAfterDone | true | 测试结束后是否删除表。 | 参数取值如下：<br />- true：表示删除表。<br />- false：表示不删除表。<br />如果在写入测试后，需要基于同一个表进行更新、点查等测试，则需要设为true。 |
|  | vacuumTableBeforeRun | false | 测试开始前是否执行vacuum操作。 | 参数取值如下：<br />- true：表示执行vacuum操作。<br />- false：表示不执行vacuum操作。<br />执行vacuum会强制触发Compaction操作。只影响INSERT模式下的数据更新场景测试，不影响FIXED_COPY模式。 |

- 然后执行如下语句进行测试：
   - 测试工具会默认将结果文件result.csv保存在根目录下，将测试过程日志文件保存在目标目录下。
   - 其中，测试模式参数包含3个取值，FIXED_COPY对应Fixed copy模式，INSERT对应Insert模式，GET对应点查模式。
```
--测试语句格式
java -jar <JAR_file_name> test_insert.conf <mode> > <log_file_location>

--示例：使用Fixed copy模式进行数据写入测试，并将日志文件存储于jar_result文件下
java -jar holo-e2e-performance-tool-1.0-SNAPSHOT.jar test_insert.conf FIXED_COPY > ./jar_result/fixed_copy_$(date '+%Y-%m-%d-%H:%M:%S').log
```
## 数据更新
### 原理说明
数据更新原理：

- 在测试过程中，测试工具会将主键id从1开始逐条递增进行数据更新。针对时间列，更新为当前时间。针对配置的其他需要更新的TEXT列（可配置全局更新或局部更新），重新写入目标长度的字符串+主键id。在达到目标时间或目标行数后停止更新，计算测试结果。

注意事项：
- 进行数据更新测试前，请先进行数据写入确保表里已有写入数据，且在数据写入阶段，deleteTableAfterDone不要为true
### 全局更新

- 创建名为test_update.conf的测试配置文件
```
-- 连接配置
holoClient.jdbcUrl=jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>
holoClient.username=<AccessKey_ID>
holoClient.password=<AccessKey_Secret>
holoClient.writeThreadSize=100

-- 写入配置
put.threadSize=8
put.testByTime=false
put.rowNumber=200000000
put.testTime=600000

-- 表配置
put.tableName=kv_test
put.columnCount=20
put.columnSize=20
put.orientation=row

-- 其他配置
put.createTableBeforeRun=false
put.deleteTableAfterDone=false
put.vacuumTableBeforeRun=false
```

   - 配置参数说明：
      - 各参数含义与数据写入场景一致
      - 相比于数据写入场景，仅需将createTableBeforeRun参数由true修改为false，其他参数均保持不变，即可开始数据更新场景的测试
- 执行如下语句进行测试：
```sql
--测试语句格式
java -jar <JAR_file_name> test_update.conf <mode> > <log_file_location>

--示例：使用Fixed copy模式进行数据写入测试，并将日志文件存储于jar_result文件下
java -jar holo-e2e-performance-tool-1.0-SNAPSHOT.jar test_update.conf FIXED_COPY > ./jar_result/fixed_copy_$(date '+%Y-%m-%d-%H:%M:%S').log
```
### 局部更新
- 创建名为test_update_part.conf的测试配置文件
```
-- 连接配置
holoClient.jdbcUrl=jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>
holoClient.username=<AccessKey_ID>
holoClient.password=<AccessKey_Secret>
holoClient.writeThreadSize=100

-- 写入配置
put.threadSize=8
put.testByTime=false
put.rowNumber=200000000
put.testTime=600000

-- 表配置
put.tableName=kv_test
put.columnCount=20
put.columnSize=20
put.writeColumnCount=10
put.orientation=row

-- 其他配置
put.createTableBeforeRun=false
put.deleteTableAfterDone=false
put.vacuumTableBeforeRun=false
```

   - 配置参数说明：
      - 相比于全局更新场景，局部更新场景需要增加参数writeColumnCount，该参数用于定义数据写入全部TEXT列中的几列。本文均将该参数设为表列数columnCount的50%。
      - 其他参数含义与全局更新场景一致，且均可保持不变。
- 执行如下语句进行测试：
```
--测试语句格式
java -jar <JAR_file_name> test_update_part.conf <mode> > <log_file_location>

--示例：使用Fixed copy模式进行数据写入测试，并将日志文件存储于jar_result文件下
java -jar holo-e2e-performance-tool-1.0-SNAPSHOT.jar test_update_part.conf FIXED_COPY > ./jar_result/fixed_copy_$(date '+%Y-%m-%d-%H:%M:%S').log
```

## 点查
### 原理说明
点查模式说明：

- 同步模式：点查接口是阻塞的，点查调用需要等待实际请求完成才返回。对于一个工作线程，每个点查请求都对应一个sql从发起到完成，一个请求结束后才能进行下一个请求。同步模式适合latency敏感、吞吐需求相对不敏感的场景。
- 异步模式：点查接口是非阻塞的，点查调用无须等待实际请求完成，会立即返回。多个点查请求异步提交后，当客户端一个工作线程中判断满足攒批大小或者提交间隔后，会把多个点查请求通过攒批方式用一条SQL批量处理。异步模式适合吞吐需求高、latency需求相对较低的场景，如Flink消费实时数据进行高吞吐的维表关联等。

点查测试原理：

- 在测试过程中，测试工具会在配置的主键范围内，随机生成目标id进行同步或异步点查。在达到目标时间后停止点查测试，计算测试结果。
### 操作步骤
说明：

- 如果自动生成的数据无法满足您需要的点查业务场景，您可以自行写入业务数据，完成主键、分布列、分段列等表属性的创建，而后根据如下步骤进行点查性能测试。
- 注意：自行创建的数据表中必须包含单列主键，主键数据类型为INT或BIGINT，同时需要注意主键的连续性。

- 创建名为test.conf的测试配置文件
```
-- 连接配置
holoClient.jdbcUrl=jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>
holoClient.username=<AccessKey_ID>
holoClient.password=<AccessKey_Secret>
holoClient.readThreadSize=16

-- 测试配置
get.threadSize=8
get.testTime=300000
get.tableName=kv_test
get.async=true
get.vacuumTableBeforeRun=true
get.keyRangeParams=L1-200000000

-- 表初始化配置
prepareGetData.rowNumber=1000000
prepareGetData.orientation=row
put.columnCount=20
put.columnSize=20
```

- 配置参数说明：
   
| 模块 | 参数 | 默认值 | 描述 | 备注 |
| --- | --- | --- | --- | --- |
| 连接配置 | 空 | jdbcUrl | Hologres的JDBC连接串，格式为jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>。 | ENDPOINT需要填写Hologres实例的VPC网络地址。 |
|  | username | 空 | 当前阿里云账号的AccessKey ID。 | 您可以登录[AccessKey 管理](https://ram.console.aliyun.com/manage/ak?spm=5176.2020520207.nav-right.dak.538b4c12VYbuIb)，获取AccessKey ID。 |
|  | password | 空 | 当前阿里云账号的AccessKey Secret。 | 您可以登录[AccessKey 管理](https://ram.console.aliyun.com/manage/ak?spm=5176.2020520207.nav-right.dak.538b4c12VYbuIb)，获取AccessKey Secret。 |
|  | readThreadSize | 1 | 查询场景的连接数。 | <br />- 异步模式下，为提高攒批效率，建议设为线程数threadSize的2-4倍。<br />- 同步模式下，为确保计算资源充分利用，建议适当调高连接数。本文设置的连接数为100。<br /> |
| 测试配置 | threadSize | 10 | 线程数。 | 针对本文测试的64核实例规格：<br />- 在异步点查模式下建议设置线程数为8。<br />- 在同步点查模式下建议设置线程数为500。<br />您可以根据实例规格适当调整该参数。 |
|  | testTime | 600000 | 测试的目标时间，单位为毫秒。 |  |
|  | tableName | holo_perf | 测试的目标表名。 |  |
|  | async | true | 点查测试的模式是否为异步。 | 参数取值如下：<br />- true：表示异步。<br />- false：表示同步。<br /> |
|  | vacuumTableBeforeRun | true | 测试开始前是否执行vacuum操作。 | 参数取值如下：<br />- true：表示执行vacuum操作。<br />- false：表示不执行vacuum操作。<br />执行vacuum会强制触发Compaction操作。 |
|  | keyRangeParams | 无 | 点查的主键参数范围，格式为<I/L><Start-End> | 参数取值如下：<br />- I/L：I表示INT类型，L表示BIGINT类型。<br />- Start：主键起始值。<br />- End：主键结束值。<br />比如L0-5000，表示BIGINT类型，主键值为0-5000。<br />点查测试过程中，测试工具会在配置的范围内随机生成目标主键进行查询。 |
| 表初始化配置 | rowNumber | 1000000 | 测试的目标行数 | |
|  | orientation | row |表的存储类型 | 参数取值如下：<br />- row：行存表<br />- row,column：行列共存表<br /> |
|  | 其他 | |除rowNumber，orientation，tableName外其他表配置可以参考写入的表配置参数| |
- 执行如下语句进行测试：
```
--测试语句格式
java -jar <JAR_file_name> test.conf <mode> > <log_file_location>

--示例：使用GET模式进行点查测试，并将日志文件存储于jar_result文件下
  同一份配置文件，先通过PREPARE_GET_DATA模式准备数据，再通过GET模式进行点查测试
java -jar holo-e2e-performance-tool-1.0-SNAPSHOT.jar test.conf PREPARE_GET_DATA
java -jar holo-e2e-performance-tool-1.0-SNAPSHOT.jar test.conf GET > ./jar_result/select_$(date '+%Y-%m-%d-%H:%M:%S').log
```

## 前缀扫描
### 原理说明
前缀扫描模式说明：
前缀扫描模式下会生成两个主键字段，id和id1, 其中id为作为表的distribution key，测试查询为select * from table where id = ?;

前缀扫描测试原理：

- 在测试过程中，测试工具会在配置的前缀主键值的范围内，随机生成目标id进行前缀扫描。在达到目标时间后停止测试，计算测试结果。
### 操作步骤
说明：

- 如果自动生成的数据无法满足您需要的点查业务场景，您可以自行写入业务数据，完成主键、分布列、分段列等表属性的创建，而后根据如下步骤进行点查性能测试。
- 注意：自行创建的数据表中必须包含单列主键，主键数据类型为INT或BIGINT，同时需要注意主键的连续性。

- 创建名为test.conf的测试配置文件
```
-- 连接配置
holoClient.jdbcUrl=jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>
holoClient.username=<AccessKey_ID>
holoClient.password=<AccessKey_Secret>
holoClient.readThreadSize=20

-- 测试配置
scan.threadSize=8
scan.testTime=300000
scan.tableName=kv_test
scan.vacuumTableBeforeRun=true
scan.keyRangeParams=L1-200000

-- 表初始化配置
prepareScanData.rowNumber=1000000
prepareScanData.orientation=row
prepareScanData.recordCountPerPrefix=100
put.columnCount=20
put.columnSize=20
```

- 配置参数说明：
   
| 模块 | 参数 | 默认值 | 描述 | 备注 |
| --- | --- | --- | --- | --- |
| 连接配置 | 空 | jdbcUrl | Hologres的JDBC连接串，格式为jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>。 | ENDPOINT需要填写Hologres实例的VPC网络地址。 |
|  | username | 空 | 当前阿里云账号的AccessKey ID。 | 您可以登录[AccessKey 管理](https://ram.console.aliyun.com/manage/ak?spm=5176.2020520207.nav-right.dak.538b4c12VYbuIb)，获取AccessKey ID。 |
|  | password | 空 | 当前阿里云账号的AccessKey Secret。 | 您可以登录[AccessKey 管理](https://ram.console.aliyun.com/manage/ak?spm=5176.2020520207.nav-right.dak.538b4c12VYbuIb)，获取AccessKey Secret。 |
|  | readThreadSize | 1 | 查询场景的连接数。 | <br />- 异步模式下，为提高攒批效率，建议设为线程数threadSize的2-4倍。<br />- 同步模式下，为确保计算资源充分利用，建议适当调高连接数。本文设置的连接数为100。<br /> |
| 测试配置 | threadSize | 10 | 线程数。 | 针对本文测试的64核实例规格：<br />- 在异步点查模式下建议设置线程数为8。<br />- 在同步点查模式下建议设置线程数为500。<br />您可以根据实例规格适当调整该参数。 |
|  | testTime | 600000 | 测试的目标时间，单位为毫秒。 |  |
|  | tableName | holo_perf | 测试的目标表名。 |  |
|  | vacuumTableBeforeRun | true | 测试开始前是否执行vacuum操作。 | 参数取值如下：<br />- true：表示执行vacuum操作。<br />- false：表示不执行vacuum操作。<br />执行vacuum会强制触发Compaction操作。 |
|  | keyRangeParams | 无 | 前缀扫描的前缀主键参数范围，格式为<I/L><Start-End> | 参数取值如下：<br />- I/L：I表示INT类型，L表示BIGINT类型。<br />- Start：主键起始值。<br />- End：主键结束值。<br />比如L0-5000，表示BIGINT类型，主键值为0-5000。<br />点查测试过程中，测试工具会在配置的范围内随机生成目标主键进行查询。 |
| 表初始化配置 | rowNumber | 1000000 | 测试的目标行数 | |
|  | orientation | row |表的存储类型 | 参数取值如下：<br />- row：行存表<br />- row,column：行列共存表<br /> |
|  | recordCountPerPrefix | 100 | 每个前缀值对应多少条数据 | |
|  | 其他 | |除rowNumber，orientation，tableName外其他表配置可以参考写入的表配置参数| |
- 执行如下语句进行测试：
```sql
--测试语句格式
java -jar <JAR_file_name> test.conf <mode> > <log_file_location>

--示例：使用SCAN模式进行前缀扫描测试，并将日志文件存储于jar_result文件下
  同一份配置文件，先通过PREPARE_SCAN_DATA模式准备数据，再通过SCAN模式进行前缀扫描测试
java -jar holo-e2e-performance-tool-1.0-SNAPSHOT.jar test.conf PREPARE_SCAN_DATA
java -jar holo-e2e-performance-tool-1.0-SNAPSHOT.jar test.conf SCAN > ./jar_result/select_$(date '+%Y-%m-%d-%H:%M:%S').log
```

# 测试结果
测试结果文件result.csv中包含如下字段：

| 字段 | 描述 | 备注 |
| --- | --- | --- |
| start | 测试开始时间 |  |
| end | 测试结束时间 |  |
| count | 测试运行的总请求数 |  |
| qps1 | 最后1分钟的平均QPS |  |
| qps5 | 最后5分钟的平均QPS |  |
| qps15 | 最后15分钟的平均QPS |  |
| latencyMean | 平均延迟 | GET/INSERT/SCAN场景收集 |
| latencyP99 | P99延迟 | GET/INSERT/SCAN场景收集场景收集 |
| latencyP999 | P999延迟 | GET/INSERT/SCAN场景收集 |
| version | 实例版本 |  |
