# holo-client-py Performance Test Tool

Hologres Python客户端（holo-client-py）的性能测试工具，用于测试数据写入、数据更新、点查、前缀扫描场景的性能。

本工具是Java性能测试工具（holo-e2e-performance-tool）的Python对应版本，配置文件格式完全兼容。

## 支持的测试模式

| 模式 | 说明 |
| --- | --- |
| INSERT | 通过HoloClient.put()进行批量写入 |
| ASYNC_INSERT | 通过AsyncHoloClient.put()进行异步批量写入 |
| FIXED_COPY | 通过COPY协议（STREAM模式）进行流式写入 |
| UPDATE | 通过HoloClient.put()进行数据更新（表中需已有数据） |
| ASYNC_UPDATE | 通过AsyncHoloClient.put()进行异步数据更新 |
| FIXED_COPY_UPDATE | 通过COPY协议进行数据更新（表中需已有数据） |
| GET | 点查性能测试 |
| SCAN | 前缀扫描性能测试（SELECT * WHERE id = ?） |
| PREPARE_GET_DATA | 为GET测试准备数据 |
| PREPARE_SCAN_DATA | 为SCAN测试准备数据 |

## 环境准备

确保已安装holo-client-py及其依赖：

```bash
pip install -e ../  # 安装holo-client-py
pip install psycopg[binary]
```

## 使用方法

```bash
python -m perf <conf_file> <MODE>
```

示例：

```bash
# 写入测试
python -m perf test_insert.conf INSERT

# Fixed copy写入测试
python -m perf test_insert.conf FIXED_COPY

# 异步写入测试
python -m perf test_insert.conf ASYNC_INSERT

# 数据更新测试（需先执行写入）
python -m perf test_update.conf UPDATE

# 点查测试（需先准备数据）
python -m perf test_get.conf PREPARE_GET_DATA
python -m perf test_get.conf GET

# 前缀扫描测试（需先准备数据）
python -m perf test_scan.conf PREPARE_SCAN_DATA
python -m perf test_scan.conf SCAN
```

## 配置文件格式

配置文件使用Java properties格式（与Java工具兼容），支持`#`和`--`开头的注释行。

### 数据写入/更新

```
-- 连接配置
holoClient.jdbcUrl=jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>
holoClient.username=<AccessKey_ID>
holoClient.password=<AccessKey_Secret>
holoClient.writeParallelism=4

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

#### 写入/更新参数说明

| 模块 | 参数 | 默认值 | 说明 |
| --- | --- | --- | --- |
| 连接配置 | jdbcUrl | 空 | Hologres JDBC连接串，格式为`jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>` |
| | username | 空 | AccessKey ID |
| | password | 空 | AccessKey Secret |
| | writeParallelism | 4 | 写入并行度（INSERT模式下holo-client内部worker数） |
| | writeBatchSize | - | 写入攒批大小 |
| | writeMaxIntervalMs | - | 写入最大攒批间隔（毫秒） |
| 写入配置 | threadSize | 10 | 写入线程数 |
| | testByTime | true | true=按时间测试，false=按行数测试 |
| | rowNumber | 1000000 | 目标写入行数（testByTime=false时生效） |
| | testTime | 600000 | 目标测试时间，毫秒（testByTime=true时生效） |
| 表配置 | tableName | holo_perf | 目标表名 |
| | columnCount | 100 | TEXT列数量（不含主键列和时间列） |
| | columnSize | 10 | 每列数据长度（byte） |
| | orientation | column | 存储类型：row / column / row,column |
| | hasPk | true | 是否有主键 |
| | shardCount | -1 | shard数，-1表示使用默认值 |
| | writeColumnCount | -1 | 局部更新时写入的列数，-1表示全部列 |
| 其他配置 | createTableBeforeRun | true | 测试前是否建表（会先删除同名表） |
| | deleteTableAfterDone | true | 测试后是否删除表 |
| | vacuumTableBeforeRun | false | 测试前是否执行vacuum |
| | dumpMemoryStat | false | 是否收集内存信息 |

#### 数据更新说明

- 更新测试前需先进行数据写入，确保表中已有数据
- 将`createTableBeforeRun`设为`false`，其他参数可保持与写入一致
- 局部更新通过`writeColumnCount`控制更新列数

### 点查测试

```
-- 连接配置
holoClient.jdbcUrl=jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>
holoClient.username=<AccessKey_ID>
holoClient.password=<AccessKey_Secret>
holoClient.readParallelism=16

-- 测试配置
get.threadSize=8
get.testTime=300000
get.tableName=kv_test
get.mode=async-with-future
get.batchSize=100
get.vacuumTableBeforeRun=true
get.keyRangeParams=L1-200000000

-- 表初始化配置（PREPARE_GET_DATA模式使用）
prepareGetData.rowNumber=1000000
prepareGetData.orientation=row
put.columnCount=20
put.columnSize=20
```

#### 点查参数说明

| 模块 | 参数 | 默认值 | 说明 |
| --- | --- | --- | --- |
| 连接配置 | readParallelism | 4 | 读取并行度，异步模式建议设为threadSize的2-4倍 |
| 测试配置 | threadSize | 10 | 测试线程数（sync模式建议较高，如500） |
| | testTime | 600000 | 测试时间（毫秒） |
| | tableName | holo_perf | 目标表名 |
| | mode | async-with-future | 点查模式（见下表） |
| | batchSize | 100 | 攒批大小 |
| | queueSize | -1 | 请求队列大小，-1表示自动（batchSize * threadSize * 2） |
| | keyRangeParams | 空 | 主键范围，格式为`<I/L><Start>-<End>`，如`L1-200000000` |
| | vacuumTableBeforeRun | true | 测试前是否执行vacuum |

#### 点查模式

Python客户端支持三种点查模式，通过`get.mode`配置：

| 模式 | 说明 | 适用场景 |
| --- | --- | --- |
| sync | 同步阻塞，每个get等待结果返回 | 延迟敏感场景 |
| async-with-future | 非阻塞提交，内部攒批执行 | 高吞吐场景（推荐） |
| async-with-coroutine | 使用asyncio协程，纯异步 | asyncio应用场景 |

### 前缀扫描测试

```
-- 连接配置
holoClient.jdbcUrl=jdbc:hologres://<ENDPOINT>:<PORT>/<DBNAME>
holoClient.username=<AccessKey_ID>
holoClient.password=<AccessKey_Secret>
holoClient.readParallelism=20

-- 测试配置
scan.threadSize=8
scan.testTime=300000
scan.tableName=kv_test
scan.vacuumTableBeforeRun=true
scan.keyRangeParams=L1-200000

-- 表初始化配置（PREPARE_SCAN_DATA模式使用）
prepareScanData.rowNumber=1000000
prepareScanData.orientation=row
prepareScanData.recordCountPerPrefix=100
put.columnCount=20
put.columnSize=20
```

#### 前缀扫描参数说明

| 模块 | 参数 | 默认值 | 说明 |
| --- | --- | --- | --- |
| 测试配置 | threadSize | 10 | 测试线程数 |
| | testTime | 600000 | 测试时间（毫秒） |
| | tableName | holo_perf | 目标表名 |
| | keyRangeParams | 空 | 前缀主键范围，格式为`<I/L><Start>-<End>` |
| | vacuumTableBeforeRun | true | 测试前是否执行vacuum |
| 表初始化 | rowNumber | 1000000 | 数据准备的总行数 |
| | orientation | row | 表存储类型：row / row,column |
| | recordCountPerPrefix | 100 | 每个前缀值对应的数据条数 |

## 测试结果

测试完成后，结果写入配置文件同目录的`result.csv`，同时输出到日志。

结果字段：

| 字段 | 说明 |
| --- | --- |
| start | 测试开始时间（毫秒时间戳） |
| end | 测试结束时间（毫秒时间戳） |
| count | 总请求数 |
| qps1 | 最近1分钟平均QPS |
| qps5 | 最近5分钟平均QPS |
| qps15 | 最近15分钟平均QPS |
| latencyMean | 平均延迟（ms） |
| latencyP99 | P99延迟（ms） |
| latencyP999 | P999延迟（ms） |
| memoryUsage | 客户端内存使用（KB） |
| version | Hologres实例版本 |
