# 通过holo-client-py读写Hologres

- [通过holo-client-py读写Hologres](#通过holo-client-py读写hologres)
  - [功能介绍](#功能介绍)
  - [holo-client-py引入](#holo-client-py引入)
  - [连接数说明](#连接数说明)
  - [数据写入](#数据写入)
    - [写入普通表](#写入普通表)
    - [写入含主键表](#写入含主键表)
    - [局部更新主键表](#局部更新主键表)
    - [基于主键删除](#基于主键删除)
  - [copy写入](#copy写入)
    - [fixed copy写入普通表](#fixed-copy写入普通表)
    - [通过Stage写入](#通过stage写入)
  - [数据查询](#数据查询)
    - [基于完整主键查询](#基于完整主键查询)
    - [Scan查询](#scan查询)
  - [异常处理](#异常处理)
  - [自定义操作](#自定义操作)
  - [附录](#附录)
    - [HoloConfig参数说明](#holoconfig参数说明)
      - [基础配置](#基础配置)
      - [写入配置](#写入配置)
      - [查询配置](#查询配置)
      - [连接配置](#连接配置)


## 功能介绍
holo-client-py是Hologres Python客户端，适用于大批量数据写入（批量、实时同步至Hologres）和高QPS点查（维表关联）场景。holo-client-py基于psycopg3实现，同时提供同步和异步两种API，使用时请确认实例剩余可用连接数。

- 查看最大连接数
```sql
select instance_max_connections();
```

- 查看已使用连接数
```sql
select count(*) from pg_stat_activity where backend_type='client backend';
```

## holo-client-py引入

- pip安装
```bash
pip install hologres-client
```

- 从源码安装
```bash
cd holo-client-py
pip install -e .
```

- 依赖要求
  - Python >= 3.8
  - psycopg[binary] >= 3.1

## 连接数说明
- HoloClient会根据write_parallelism和read_parallelism创建对应数量的连接
- 空闲超过connection_max_idle_ms的连接会被释放

## 数据写入
建议项目中创建HoloClient/AsyncHoloClient单例，通过write_parallelism控制写入并发。

- 同步客户端（`HoloClient`）：内部使用多进程+共享内存实现并行写入
- 异步客户端（`AsyncHoloClient`）：内部使用多个asyncio writer task并发写入，适用于asyncio应用

### 写入普通表

**同步接口：**
```python
from hologres import HoloClient, HoloConfig, Put

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
    write_parallelism=4,  # 同步模式下启动4个worker进程并行写入
)

with HoloClient(config) as client:
    schema = client.get_table_schema("t0")
    put = Put(schema)
    put.set_object("id", 1)
    put.set_object("name", "name0")
    put.set_object("address", "address0")
    client.put(put)
    # 强制提交所有未提交put请求；HoloClient内部也会根据write_batch_size、write_batch_byte_size、write_max_interval_ms自动提交
    # client.flush()
```

**异步接口：**

使用`asyncio.create_task`批量提交put请求，让writer task充分攒批，获得最高吞吐。避免逐个`await client.put()`导致每批仅1条请求。

```python
import asyncio
from hologres import AsyncHoloClient, HoloConfig, Put

async def main():
    config = HoloConfig(
        host="host",
        port=80,
        database="db",
        username="username",
        password="password",
        write_parallelism=4,  # 异步模式下启动4个writer task并发刷入
    )

    async with AsyncHoloClient(config) as client:
        schema = await client.get_table_schema("t0")

        # 批量提交put请求，不逐个await
        tasks = []
        for i in range(100):
            put = Put(schema)
            put.set_object("id", i)
            put.set_object("name", f"name{i}")
            put.set_object("address", f"address{i}")
            tasks.append(asyncio.create_task(client.put(put)))

        # 统一等待所有结果
        await asyncio.gather(*tasks)
        await client.flush()

asyncio.run(main())
```

### 写入含主键表

**同步接口：**
```python
from hologres import HoloClient, HoloConfig, Put, OnConflictAction

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
    on_conflict_action=OnConflictAction.INSERT_OR_REPLACE,  # 配置主键冲突时策略
)

with HoloClient(config) as client:
    # create table t0(id int not null, name0 text, address text, primary key(id))
    schema = client.get_table_schema("t0")
    put = Put(schema)
    put.set_object("id", 1)
    put.set_object("name0", "name0")
    put.set_object("address", "address0")
    client.put(put)

    put = Put(schema)
    put.set_object("id", 1)
    put.set_object("name0", "newName")
    put.set_object("address", "newAddress")
    client.put(put)
    # client.flush()
```

**异步接口：**

使用`asyncio.create_task`批量提交put请求，让writer task充分攒批，获得最高吞吐。避免逐个`await client.put()`导致每批仅1条请求。

```python
import asyncio
from hologres import AsyncHoloClient, HoloConfig, Put, OnConflictAction

async def main():
    config = HoloConfig(
        host="host",
        port=80,
        database="db",
        username="username",
        password="password",
        on_conflict_action=OnConflictAction.INSERT_OR_REPLACE,
    )

    async with AsyncHoloClient(config) as client:
        # create table t0(id int not null, name0 text, address text, primary key(id))
        schema = await client.get_table_schema("t0")

        # 批量提交put请求，不逐个await
        tasks = []
        for i in range(100):
            put = Put(schema)
            put.set_object("id", i)
            put.set_object("name0", f"name{i}")
            put.set_object("address", f"address{i}")
            tasks.append(asyncio.create_task(client.put(put)))

        # 统一等待所有结果
        await asyncio.gather(*tasks)
        await client.flush()

asyncio.run(main())
```

### 局部更新主键表

**同步接口：**
```python
from hologres import HoloClient, HoloConfig, Put, OnConflictAction

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
    on_conflict_action=OnConflictAction.INSERT_OR_UPDATE,  # 局部更新
)

with HoloClient(config) as client:
    # create table t0(id int not null, name0 text, address text, primary key(id))
    schema = client.get_table_schema("t0")

    # 只put id和name0两个字段，当主键冲突则更新对应字段，否则写入
    put = Put(schema)
    put.set_object("id", 1)
    put.set_object("name0", "name0")
    client.put(put)

    put = Put(schema)
    put.set_object("id", 1)
    put.set_object("name0", "newName")
    client.put(put)
    # client.flush()
```

**异步接口：**

使用`asyncio.create_task`批量提交put请求，让writer task充分攒批，获得最高吞吐。避免逐个`await client.put()`导致每批仅1条请求。

```python
import asyncio
from hologres import AsyncHoloClient, HoloConfig, Put, OnConflictAction

async def main():
    config = HoloConfig(
        host="host",
        port=80,
        database="db",
        username="username",
        password="password",
        on_conflict_action=OnConflictAction.INSERT_OR_UPDATE,
    )

    async with AsyncHoloClient(config) as client:
        # create table t0(id int not null, name0 text, address text, primary key(id))
        schema = await client.get_table_schema("t0")

        # 批量提交put请求，不逐个await
        tasks = []
        for i in range(100):
            put = Put(schema)
            put.set_object("id", i)
            put.set_object("name0", f"name{i}")
            tasks.append(asyncio.create_task(client.put(put)))

        # 统一等待所有结果
        await asyncio.gather(*tasks)
        await client.flush()

asyncio.run(main())
```

### 基于主键删除
DELETE占比提高会降低整体的每秒写入。

**同步接口：**
```python
from hologres import HoloClient, HoloConfig, Put, MutationType, OnConflictAction

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
    on_conflict_action=OnConflictAction.INSERT_OR_REPLACE,
)

with HoloClient(config) as client:
    # create table t0(id int not null, name0 text, address text, primary key(id))
    schema = client.get_table_schema("t0")
    put = Put(schema)
    put.mutation_type = MutationType.DELETE
    put.set_object("id", 1)
    client.put(put)
    # client.flush()
```

**异步接口：**

使用`asyncio.create_task`批量提交delete请求，让writer task充分攒批，获得最高吞吐。避免逐个`await client.put()`导致每批仅1条请求。

```python
import asyncio
from hologres import AsyncHoloClient, HoloConfig, Put, MutationType, OnConflictAction

async def main():
    config = HoloConfig(
        host="host",
        port=80,
        database="db",
        username="username",
        password="password",
        on_conflict_action=OnConflictAction.INSERT_OR_REPLACE,
    )

    async with AsyncHoloClient(config) as client:
        # create table t0(id int not null, name0 text, address text, primary key(id))
        schema = await client.get_table_schema("t0")

        # 批量提交delete请求，不逐个await
        tasks = []
        for i in range(100):
            put = Put(schema)
            put.mutation_type = MutationType.DELETE
            put.set_object("id", i)
            tasks.append(asyncio.create_task(client.put(put)))

        # 统一等待所有结果
        await asyncio.gather(*tasks)
        await client.flush()

asyncio.run(main())
```

## copy写入
fixed copy为Hologres 1.3.x引入，相比HoloClient.put方法，fixed copy方式可以获得更高的吞吐（流模式），更低的数据延迟，更低的客户端内存消耗（不攒批）。

对于无delete的实时写入场景，建议使用fixed copy写入。

### fixed copy写入普通表

**同步接口：**
```python
from hologres import HoloClient, HoloConfig, Put, CopyFormat, CopyMode, OnConflictAction

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
    on_conflict_action=OnConflictAction.INSERT_OR_UPDATE,  # copy_writer使用HoloConfig中的冲突策略
)

with HoloClient(config) as client:
    # CREATE TABLE copy_demo (id INT NOT NULL, name TEXT NOT NULL, address TEXT, PRIMARY KEY(id));
    columns = ["id", "name"]

    with client.copy_writer(
        "copy_demo",
        columns=columns,
        fmt=CopyFormat.BINARY,
        mode=CopyMode.STREAM,
    ) as writer:
        for i in range(10):
            put = Put(writer.schema)
            put.set_object("id", i)
            put.set_object("name", "name0")
            writer.write(put)
```

**异步接口：**
```python
import asyncio
from hologres import AsyncHoloClient, HoloConfig, Put, CopyFormat, CopyMode, OnConflictAction

async def main():
    config = HoloConfig(
        host="host",
        port=80,
        database="db",
        username="username",
        password="password",
        on_conflict_action=OnConflictAction.INSERT_OR_UPDATE,
    )

    async with AsyncHoloClient(config) as client:
        columns = ["id", "name"]

        async with await client.copy_writer(
            "copy_demo",
            columns=columns,
            fmt=CopyFormat.BINARY,
            mode=CopyMode.STREAM,
        ) as writer:
            for i in range(10):
                put = Put(writer.schema)
                put.set_object("id", i)
                put.set_object("name", "name0")
                await writer.write(put)

asyncio.run(main())
```

### 通过Stage写入
holo-client-py支持通过CopyStageWriter将数据写入Hologres内部Stage，再通过INSERT语句将数据从Stage加载到目标表。该方式适合需要先暂存再批量导入的场景，具有原子性。

```python
from hologres import HoloClient, HoloConfig, Put

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
)

with HoloClient(config) as client:
    # CREATE TABLE copy_stage_demo (id INT NOT NULL, name TEXT NOT NULL, address TEXT, PRIMARY KEY(id));
    columns = ["id", "name", "address"]
    stage_name = "my_stage"

    # 创建内部Stage，TTL为7200秒（2小时），超时后自动清理
    client.create_stage(stage_name, ttl_seconds=7200)

    try:
        with client.copy_stage_writer(
            "copy_stage_demo",
            stage_name,
            columns=columns,
        ) as writer:
            for i in range(10):
                put = Put(writer.schema)
                put.set_object("id", i)
                put.set_object("name", f"name{i}")
                put.set_object("address", f"address{i}")
                writer.write(put)
    finally:
        # 清理临时Stage（不清理也会根据TTL自动清理）
        client.drop_stage(stage_name)
```

## 数据查询
### 基于完整主键查询

**同步接口：**
```python
from hologres import HoloClient, HoloConfig, Get

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
    read_parallelism=4,  # 同步模式下启动4个reader线程并行查询
)

with HoloClient(config) as client:
    # create table t0(id int not null, name0 text, address text, primary key(id))
    schema = client.get_table_schema("t0")

    get = Get.builder(schema).set_primary_key("id", 1).build()
    record = client.get(get)
    if record:
        print(record.get_object("name0"))
```

**非阻塞接口（async_get）：**

`async_get`返回`concurrent.futures.Future`，适用于同步客户端下需要高QPS点查的场景。调用方可以先批量提交多个get请求，再统一等待结果，从而让reader线程充分攒批执行。

```python
from hologres import HoloClient, HoloConfig, Get

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
    read_parallelism=4,
)

with HoloClient(config) as client:
    # create table t0(id int not null, name0 text, address text, primary key(id))
    schema = client.get_table_schema("t0")

    # 批量提交get请求，不阻塞等待结果
    futures = []
    for i in range(100):
        get = Get.builder(schema).set_primary_key("id", i).build()
        futures.append(client.async_get(get))

    # 统一等待所有结果
    for future in futures:
        record = future.result()
        if record:
            print(record.get_object("name0"))
```

**异步接口：**

使用`asyncio.create_task`批量提交get请求，让reader task充分攒批，获得最高吞吐。避免逐个`await client.get()`导致每批仅1条请求。

```python
import asyncio
from hologres import AsyncHoloClient, HoloConfig, Get

async def main():
    config = HoloConfig(
        host="host",
        port=80,
        database="db",
        username="username",
        password="password",
        read_parallelism=4,  # 异步模式下启动4个reader task并发查询
    )

    async with AsyncHoloClient(config) as client:
        schema = await client.get_table_schema("t0")

        # 批量提交get请求，不逐个await
        tasks = []
        for i in range(100):
            get = Get.builder(schema).set_primary_key("id", i).build()
            tasks.append(asyncio.create_task(client.get(get)))

        # 统一等待所有结果
        records = await asyncio.gather(*tasks)
        for record in records:
            if record:
                print(record.get_object("name0"))

asyncio.run(main())
```

### Scan查询

**同步接口：**
```python
from hologres import HoloClient, HoloConfig, Scan, SortKeys

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
)

with HoloClient(config) as client:
    # create table t0(id int not null, name0 text, address text, primary key(id))
    schema = client.get_table_schema("t0")

    scan = (Scan.builder(schema)
            .add_equal_filter("id", 102)
            .add_range_filter("name0", start="3", end="4")
            .with_selected_columns(["address"])
            .build())
    # 等同于 select address from t0 where id=102 and name0>='3' and name0<'4' order by id
    records = client.scan(scan)
    for record in records:
        print(record.get_object("address"))

    # 不排序
    scan = (Scan.builder(schema)
            .add_equal_filter("id", 102)
            .add_range_filter("name0", start="3", end="4")
            .with_selected_columns(["address"])
            .set_sort_keys(SortKeys.NONE)
            .build())
    records = client.scan(scan)
```

**异步接口：**
```python
import asyncio
from hologres import AsyncHoloClient, HoloConfig, Scan

async def main():
    config = HoloConfig(
        host="host",
        port=80,
        database="db",
        username="username",
        password="password",
    )

    async with AsyncHoloClient(config) as client:
        schema = await client.get_table_schema("t0")

        scan = (Scan.builder(schema)
                .add_equal_filter("id", 102)
                .add_range_filter("name0", start="3", end="4")
                .with_selected_columns(["address"])
                .build())
        records = await client.scan(scan)
        for record in records:
            print(record.get_object("address"))

asyncio.run(main())
```

## 性能比较

### 写入性能

| 写入方式 | 吞吐 | 延迟 | 适用场景 |
| --- | --- | --- | --- |
| Fixed Copy（同步） | 最高 | 最低 | 无delete的实时写入，追求最大吞吐 |
| Put（同步，write_parallelism>1） | 高 | 低 | 通用写入，支持delete和主键冲突策略 |
| Put（异步，write_parallelism>1） | 中 | 中 | asyncio应用，受GIL限制吞吐低于同步多进程 |
| Put（同步，write_parallelism=1） | 低 | 低 | 低吞吐场景，资源占用最少 |

说明：
- 同步Put通过多进程+共享内存实现并行，不受GIL限制，提高`write_parallelism`可线性提升吞吐
- 异步Put的生产者与flush共享同一事件循环，`write_parallelism`提升带来的收益有限
- Fixed Copy使用流式协议，绕过SQL构建和攒批，单连接即可获得最高吞吐

### 查询性能

| 查询方式 | 吞吐 | 延迟 | 适用场景 |
| --- | --- | --- | --- |
| async_get（非阻塞Future，read_parallelism>1） | 最高 | 高 | 多线程生产者高QPS点查，追求最大吞吐 |
| AsyncHoloClient.get + create_task批量提交 | 最高 | 中 | asyncio应用高QPS点查 |
| get（同步阻塞，read_parallelism>1） | 中 | 低 | 同步应用的普通点查，延迟优先 |
| Scan | 低 | 中 | 前缀范围扫描，返回多行 |

说明：
- `async_get()`返回`concurrent.futures.Future`，多线程可批量提交后统一等待，reader线程充分攒批，吞吐最高（~17K QPS）
- `AsyncHoloClient.get()`配合`asyncio.create_task`批量提交可达同等吞吐（~16K QPS）；若逐个`await`则每批仅1条请求，吞吐退化至~3.4K QPS
- `get()`每次阻塞等待单条结果返回，reader线程无法充分攒批，吞吐受限于单条往返延迟（~3.3K QPS）
- 提高`read_parallelism`增加reader线程/task数，对批量提交模式吞吐提升显著
- Scan为单次RPC返回多行，吞吐取决于结果集大小和网络延迟

## 异常处理
```python
from hologres import HoloClient, HoloConfig, Put
from hologres.exceptions import HoloClientException, HoloClientWithDetailsException

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
)

with HoloClient(config) as client:
    schema = client.get_table_schema("t0")
    put = Put(schema)
    put.set_object("id", 1)
    put.set_object("name", "name0")

    try:
        client.put(put)
        client.flush()
    except HoloClientWithDetailsException as e:
        # 包含失败记录详情的异常
        for record, cause in e.details:
            print(f"写入失败的记录: {record}, 原因: {cause}")
    except HoloClientException as e:
        # 非HoloClientWithDetailsException的异常一般是fatal的
        raise
```

## 自定义操作
```python
from hologres import HoloClient, HoloConfig

config = HoloConfig(
    host="host",
    port=80,
    database="db",
    username="username",
    password="password",
)

with HoloClient(config) as client:
    client.sql("CREATE TABLE t0(id int)")
```

## 附录
### HoloConfig参数说明
#### 基础配置
| 参数名 | 默认值 | 说明 |
| --- | --- | --- |
| host | 无 | 必填，Hologres实例地址 |
| port | 无 | 必填，端口号 |
| database | 无 | 必填，数据库名 |
| username | 无 | 必填，用户名 |
| password | 无 | 必填，密码 |
| app_name | holo-client-py | 连接的application_name参数 |

#### 写入配置
| 参数名 | 默认值 | 说明 |
| --- | --- | --- |
| write_parallelism | 4 | 处理put方法请求的最大并发数。同步模式下为worker进程数，异步模式下为writer task数 |
| on_conflict_action | INSERT_OR_REPLACE | 当INSERT目标表为有主键的表时采用不同策略：<br>INSERT_OR_IGNORE 当主键冲突时不写入<br>INSERT_OR_UPDATE 当主键冲突时更新相应列<br>INSERT_OR_REPLACE 当主键冲突时更新所有列 |
| write_batch_size | 512 | 每个写入线程的最大批次大小 |
| write_batch_byte_size | 2097152 (2MB) | 每个写入线程的最大批次字节大小 |
| write_batch_total_byte_size | 20971520 (20MB) | 所有表最大批次字节大小 |
| write_max_interval_ms | 10000 | 距离上次提交超过此值会触发一次批量提交（毫秒） |
| enable_deduplication | true | 写入时是否对攒批数据做去重 |
| enable_generate_binlog | true | 关闭时，写入的数据不会生成binlog |
| remove_u0000_in_text | true | 写入Text列时，是否剔除字符串中的\u0000 |

#### 查询配置
| 参数名 | 默认值 | 说明 |
| --- | --- | --- |
| read_parallelism | 4 | 点查并发数。同步模式下为reader线程数，异步模式下为reader task数 |
| read_batch_size | 128 | reader每次攒批执行的最大Get数量 |
| read_batch_queue_size | 256 | 异步模式下的请求缓冲队列大小 |
| read_timeout_ms | 0 | Get操作的超时时间，0表示不超时 |

#### 连接配置
| 参数名 | 默认值 | 说明 |
| --- | --- | --- |
| retry_count | 3 | 当连接故障时，写入和查询的重试次数 |
| retry_sleep_init_ms | 1000 | 重试等待时间基础值（毫秒） |
| retry_sleep_step_ms | 10000 | 重试等待时间递增步长（毫秒） |
| connection_max_idle_ms | 60000 | 数据库连接的最大空闲时间，超过将被释放（毫秒） |
| meta_cache_ttl_ms | 60000 | getTableSchema信息的本地缓存时间（毫秒） |
| use_fixed_fe | false | 开启后，Get/Put将使用Fixed FE轻量级连接 |
