# 通过 Hologres DataFrame API 处理数据

Hologres DataFrame API（`hologres-dataframe`）是面向 Hologres 的 Python DataFrame 接口。接口设计遵循 pandas 和主流 Python DataFrame API 的使用习惯，转换操作只构建惰性逻辑计划，在触发 Action 时编译为 PostgreSQL 方言 SQL 并下推到 Hologres 执行，计算在 Hologres 内完成，只将最终结果返回 Python。

主要能力包括：

- 使用 `select`、`filter`、`group_by`、`join` 等链式接口构建查询。
- 从 Hologres 表、SQL、Python 数据、pandas、本地文件和 OSS 文件创建 DataFrame。
- 将查询结果追加、UPSERT 或覆盖写入 Hologres 表。
- 支持 Dynamic Table、Serverless Computing、Hologres AI Function。
- 将 Python UDF/UDTF 自动部署为 Function Compute Remote UDX。

> **说明** 该 SDK 仅提供 snake_case 接口（例如 `group_by`），不提供 `groupBy` 等驼峰别名。

## 目录

- [前提条件](#前提条件)
- [连接 Hologres](#连接-hologres)
  - [基本用法](#基本用法)
  - [连接池参数](#连接池参数)
  - [切换 Schema 和 Database](#切换-schema-和-database)
- [创建 DataFrame](#创建-dataframe)
  - [从 Hologres 表创建](#从-hologres-表创建)
  - [从 SQL 查询创建](#从-sql-查询创建)
  - [从 Python 数据创建](#从-python-数据创建)
  - [从 pandas DataFrame 创建](#从-pandas-dataframe-创建)
  - [从本地文件创建](#从本地文件创建)
- [查询与转换数据](#查询与转换数据)
  - [列表达式](#列表达式)
  - [投影、过滤与排序](#投影过滤与排序)
  - [分组聚合](#分组聚合)
  - [Join 与 Union](#join-与-union)
  - [数组展开](#数组展开)
- [获取查询结果](#获取查询结果)
- [写入数据到 Hologres 表](#写入数据到-hologres-表)
  - [追加写入](#追加写入)
  - [主键冲突处理](#主键冲突处理)
  - [覆盖写入](#覆盖写入)
- [其他重点功能](#其他重点功能)
  - [读写 OSS 文件](#读写-oss-文件)
  - [Serverless Computing](#serverless-computing)
  - [Dynamic Table](#dynamic-table)
  - [AI Function](#ai-function)
  - [Function Compute UDF 与 UDTF](#function-compute-udf-与-udtf)
- [Catalog 与原生 SQL](#catalog-与原生-sql)
- [数据类型映射](#数据类型映射)
- [异常处理](#异常处理)

---

## 前提条件

- 已创建 Hologres 实例并获取连接信息（Endpoint、端口、数据库名称、AccessKey）。
- Python 版本 >= 3.9。

安装 `hologres-dataframe`：

```bash
pip install hologres-dataframe
```

安装指定版本：

```bash
pip install hologres-dataframe==0.1.1
```

当前最新版本为 `0.1.1`。依赖要求如下：

| 依赖包 | 最低版本 |
| --- | --- |
| `hologres-client` | >= 0.1.2 |
| `psycopg[binary,pool]` | >= 3.1 |
| `pandas` | >= 1.3 |
| `pyarrow` | >= 12.0 |
| `alibabacloud-fc20230330` | >= 4.7.9, < 5 |

> **说明** 基础安装已包含 `from_pandas()`、`to_pandas()` 和本地 CSV/JSON/Parquet 文件读取所需依赖，也默认包含 Function Compute UDF/UDTF 自动部署 SDK。已有的 `hologres-dataframe[fc]` 安装命令仍然兼容，但不再需要额外指定 `[fc]`。

---

## 连接 Hologres

### 基本用法

`Session` 管理连接池、数据库上下文、临时 Stage/表和 Function Compute 配置。建议作为上下文管理器使用，退出时自动释放连接并清理临时对象。

```python
import hologres.dataframe as hg

with hg.connect(
    host="xxx.hologres.aliyuncs.com",
    port=80,
    dbname="my_db",
    user="<access_id>",
    password="<access_key>",
    sslmode="require",
) as session:
    session.table("public.orders").limit(10).show()
```

### 连接池参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `min_size` | `1` | 连接池最小连接数。 |
| `max_size` | `10` | 连接池最大连接数，也是并发上限。 |
| `timeout` | `30.0` | 等待可用连接的最长秒数。 |
| `max_idle` | `600.0` | 空闲连接回收时间（秒）。 |
| `max_lifetime` | `3600.0` | 单个连接最大生命周期（秒）。 |
| `application_name` | `hologres-dataframe` | `pg_stat_activity` 中显示的应用名。 |
| `fc_config` | `None` | Function Compute UDF/UDTF 部署配置，详见 [Function Compute UDF 与 UDTF](#function-compute-udf-与-udtf)。 |

其他关键字参数会透传给 psycopg/libpq，例如 `sslmode`、`connect_timeout`、`keepalives` 和 `options`。

### 切换 Schema 和 Database

```python
session.use_schema("analytics")
print(session.current_schema())

session.use_database("another_db")
print(session.current_database())
```

> **说明** `use_schema()` 修改当前 `search_path`。PostgreSQL 连接绑定 Database，因此 `use_database()` 会关闭当前连接池并使用相同连接参数重建。

---

## 创建 DataFrame

### 从 Hologres 表创建

```python
orders = session.table("public.orders")
```

`session.table()` 会读取并缓存表 Schema，返回惰性 DataFrame。

### 从 SQL 查询创建

```python
paid_orders = session.sql(
    "SELECT order_id, user_id, amount FROM public.orders WHERE status = 'paid'"
)
```

`session.sql()` 返回惰性 DataFrame，原生 SQL 在触发 Action 时执行。

### 从 Python 数据创建

推荐通过 Session 创建，使 DataFrame 可以直接执行 Action 或写入操作：

```python
df = session.from_dict({"id": [1, 2], "name": ["alice", "bob"]})

df = session.from_records(
    [(1, "alice"), (2, "bob")],
    columns=["id", "name"],
)
```

小数据会编译为 SQL `VALUES`。超过 `inline_limit` 时自动转为 Arrow 数据并上传到 Hologres。

> **说明** 顶层 `hg.from_dict()` 和 `hg.from_records()` 适合仅构建 SQL 的场景。未绑定 Session 的 DataFrame 不能执行 Action。

### 从 pandas DataFrame 创建

```python
df = session.from_pandas(pdf)
df.write("public.target")
```

数据在首次 Action 时上传。Hologres 4.1 及以上版本使用 Internal Stage，旧版本使用临时表兼容。Session 关闭时自动清理框架创建的 Stage 或临时表。

### 从本地文件创建

支持 CSV、JSON、Parquet 格式，通过 pyarrow 读取。文件读取发生在调用 `read_files()` 时，上传延迟到首次 Action。

```python
csv_df = session.read_files("data/*.csv")
parquet_df = session.read_files("data/orders.parquet")

csv_df.group_by("region").count().show()
```

---

## 查询与转换数据

所有转换操作均为惰性执行，只构建逻辑计划，不会立即访问 Hologres。

### 列表达式

`hg.col()` 引用列，`hg.lit()` 创建字面量。表达式中包含 `Column` 时只构建 SQL 表达式，不会在 Python 中逐行计算。

```python
amount = hg.col("amount")

amount * 0.06
amount.between(100, 1000)
hg.col("region").isin("east", "west")
hg.col("name").ilike("ali%")
(amount > 100) & (hg.col("status") == "paid")
```

> **说明** 逻辑组合使用 `&`、`|`、`~`，每个比较条件需要加括号。Python 的 `and`、`or`、`not` 不能重载，不适用于 Column 表达式。

### 投影、过滤与排序

```python
result = (
    session.table("public.orders")
    .filter((hg.col("amount") > 100) & hg.col("region").is_not_null())
    .with_columns(
        {
            "tax": hg.col("amount") * 0.06,
            "amount_with_tax": hg.col("amount") * 1.06,
        }
    )
    .drop("internal_note")
    .rename({"region": "sales_region"})
    .select("order_id", "sales_region", "amount", "tax")
    .distinct()
    .order_by(hg.col("amount").desc())
    .limit(100)
    .offset(20)
)
```

> **说明** `drop()` 和 `rename()` 只修改查询投影，不会执行 `ALTER TABLE`。

### 分组聚合

使用 `group_by()` 和 `agg()` 进行分组聚合计算：

```python
summary = (
    session.table("public.orders")
    .group_by("region")
    .agg(
        hg.sum("amount").alias("total_amount"),
        hg.avg("amount").alias("avg_amount"),
        hg.count("*").alias("order_count"),
    )
)
```

单一聚合可使用快捷方法：

```python
session.table("public.orders").group_by("region").sum("amount")
session.table("public.orders").group_by("region").count()
```

对于不常用的 SQL 函数或特殊 SQL 语法，可以使用 `hg.function()` 和 `hg.expr()`：

```python
df.group_by("region").agg(
    hg.function("stddev", hg.col("amount")).alias("amount_stddev"),
    hg.expr("count(*) filter (where status = 'paid')").alias("paid_count"),
)
```

### Join 与 Union

```python
orders = session.table("public.orders")
users = session.table("public.users")

# 相同列名 Join
same_name_key = orders.join(users, on="user_id", how="left")

# 不同列名 Join
different_key = orders.join(
    users,
    on=orders["buyer_id"] == users["user_id"],
    how="inner",
)

# Union
all_rows = orders.union_all(session.table("public.orders_archive"))
deduplicated = orders.union(session.table("public.orders_archive"))
```

> **说明** `on` 传字符串或字符串列表时编译为 `USING`，传 Column 表达式时编译为 `ON`。`how` 支持 `inner`、`left`、`right`、`full`。

### 数组展开

```python
session.table("public.documents").explode("tags").show()
session.table("public.documents").explode("tags", outer=True).show()
```

`explode()` 编译为 Hologres 支持的 SELECT-list `unnest()`。设置 `outer=True` 会保留数组为空或 NULL 的原始行。

---

## 获取查询结果

DataFrame 的 Action 方法会触发实际执行，将 SQL 下推到 Hologres 并获取结果。

| Action | 返回值 | 适用场景 |
| --- | --- | --- |
| `show(n=10)` | `None` | 在终端查看少量结果。 |
| `collect()` | `list[Row]` | 将完整结果加载到内存。 |
| `to_pandas()` | `pandas.DataFrame` | 进入 pandas 处理流程。 |
| `count()` | `int` | 服务端统计行数。 |
| `first()` | `Row \| None` | 获取第一行。 |
| `take(n)` | `list[Row]` | 获取前 n 行。 |
| `iter_rows(batch_size=1000)` | Row 迭代器 | 分批读取大结果集。 |

```python
for row in session.table("public.orders").iter_rows(batch_size=2000):
    print(row.order_id, row.amount)
```

> **说明** `collect()` 和 `to_pandas()` 会把完整结果加载到客户端内存，大结果集应优先使用 `iter_rows()` 分批读取。

---

## 写入数据到 Hologres 表

目标表必须已经存在，建表和改表请使用 `session.sql()`。`write()` 是立即执行的 Action，数据统一通过 `INSERT ... SELECT` 写入。

### 追加写入

```python
source = session.table("public.orders_staging").select("order_id", "user_id", "amount")
source.write("public.orders")
```

内存数据也使用相同接口：

```python
session.from_records(
    [(1, "east", 100), (2, "west", 200)],
    columns=["id", "region", "amount"],
).write("public.orders")
```

### 主键冲突处理

通过 `on_conflict` 参数指定主键冲突时的处理策略：

```python
# 主键冲突时忽略
df.write("public.target", on_conflict=("ignore", ["id"]))

# 只更新 DataFrame 中提供的非主键列
df.write("public.target", on_conflict=("update", ["id"]))

# 用 DataFrame 行替换目标行，未提供的目标列写 NULL
df.write("public.target", on_conflict=("replace", ["id"]))
```

> **说明** `keys` 必须与目标表主键一致。

### 覆盖写入

```python
df.write("public.target", overwrite=True)

df.write(
    "public.partitioned_target",
    overwrite=True,
    partition={"ds": "2026-08-19"},
)
```

Hologres 3.1 及以上版本使用原生 `INSERT OVERWRITE`，低版本自动使用事务内 `TRUNCATE/DELETE + INSERT` 兼容方案。

> **说明** `overwrite=True` 与 `on_conflict` 互斥，不能同时使用。目标表启用 Binlog 时，overwrite 不能生成完整逐行 CDC 记录，默认会拒绝写入。确认下游允许 Binlog 缺口后，可以显式传入 `allow_binlog_gap=True`。

---

## 其他重点功能

### 读写 OSS 文件

Hologres 4.1 及以上版本可以通过 `EXTERNAL_FILES` 读取和导出 OSS 中的文件。

#### 读取 OSS 文件

支持 CSV、Parquet 和 ORC 格式：

```python
oss_df = session.read_files(
    "oss://bucket/input/",
    format="parquet",
    oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
    role_arn="acs:ram::123:role/hologres-oss-role",
)

oss_df.filter(hg.col("amount") > 100).show()
```

#### 导出查询结果到 OSS

```python
session.table("public.orders").write_files(
    "oss://bucket/output/",
    format="csv",
    oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
    role_arn="acs:ram::123:role/hologres-oss-role",
    target_file_size_mb=128,
)
```

> **说明** 写 OSS 仅支持 CSV 格式，路径必须是 `oss://` 协议。`role_arn` 是 Hologres 访问 OSS 时扮演的 RAM Role，已通过实例凭据授权时可以不传。如需导出为本地文件，可先调用 `to_pandas()` 再使用 pandas 的文件接口。

### Serverless Computing

通过 `serverless()` 方法将查询标记为使用 Serverless Computing 资源执行。该方法为惰性标记，返回新的 DataFrame，执行 Action 时框架在同一事务和连接上设置 `SET LOCAL`，不会将配置泄漏到连接池中的后续查询。

```python
rows = (
    session.table("public.orders")
    .filter(hg.col("amount") > 100)
    .serverless(priority=4, required_cores=64, max_cores=128)
    .collect()
)
```

参数说明：

| 参数 | 取值范围 | 说明 |
| --- | --- | --- |
| `priority` | 1-5 | 查询优先级，5 为最高。 |
| `required_cores` | 0-102400 | 请求的 Core 数，0 表示自动估算。 |
| `max_cores` | 0-102400 | 单查询 Core 上限，0 表示不设置查询级上限。 |

> **说明** Hologres 仍会判断 SQL 是否符合 Serverless 执行条件。不支持相关 GUC 的实例会直接报错，客户端不会静默回退。

### Dynamic Table

通过 `as_dynamic_table()` 将 DataFrame 查询定义为 Dynamic Table。该方法是立即执行的 DDL Action，返回 `None`。

```python
summary = (
    session.table("public.orders")
    .join(session.table("public.users"), on="user_id", how="left")
    .select("order_id", "user_id", "user_name")
)

summary.as_dynamic_table(
    "public.dt_orders",
    freshness="10 minutes",
    refresh_mode="incremental",
    auto_refresh=True,
    mode="ignore",
    options={"cdc_format": "binlog"},
)
```

`mode` 参数控制目标表已存在时的行为：

| mode | 行为 |
| --- | --- |
| `ignore` | 默认值，目标存在时不处理。 |
| `errorifexists` | 目标存在时由 Hologres 报错。 |
| `replace` | 在事务中删除并重建。 |

手动刷新和查询 Dynamic Table：

```python
session.refresh_table("public.dt_orders")
session.table("public.dt_orders").show()
```

### AI Function

`hg.ai` 将 Hologres AI Function 暴露为惰性 Column 表达式，可以在 DataFrame 查询中直接使用 AI 能力。

```python
# 文本生成
questions = session.table("public.questions")
questions.select(
    "question",
    hg.ai.gen(
        hg.function("concat", hg.lit("请简要回答："), hg.col("question"))
    ).alias("answer"),
).show()

# 文本向量化
session.table("public.docs").select(
    "id",
    hg.ai.embed(hg.col("content")).alias("embedding"),
).show()

# 情感分析
session.table("public.comments").select(
    "comment",
    hg.ai.sentiment(hg.col("comment")).alias("sentiment"),
).show()
```

除上述示例外，还支持 `rank`、`chunk`、`classify`、`extract`、`mask`、`fix_grammar`、`summarize`、`translate`、`similarity`、`parse_document`、`to_file` 和 `prompt`。具体函数可用性取决于 Hologres 实例版本和模型配置。

### Function Compute UDF 与 UDTF

Hologres 没有内嵌 Python Runtime。`@hg.udf` 和 `@hg.udtf` 会在首次使用它们的 Action 执行前，将 Python 代码自动部署到 Function Compute，再注册为 `LANGUAGE function_compute` Remote UDX。

#### 配置 Function Compute

FC 配置放在 Session 中，AccessKey 使用阿里云标准凭据链，不要写在装饰器中：

```python
session = hg.connect(
    host="xxx.hologres.aliyuncs.com",
    port=80,
    dbname="my_db",
    user="<access_id>",
    password="<access_key>",
    fc_config={
        "endpoint": "123.cn-hangzhou-internal.fc.aliyuncs.com",
        "region": "cn-hangzhou",
        "role_arn": "acs:ram::123:role/fc-execution-role",
    },
)
```

#### 标量 UDF

```python
@hg.udf(packages=["numpy"])
def grade(score: int) -> str:
    return "high" if score >= 80 else "low"

session.table("public.scores").select(
    "id", grade("score").alias("grade")
).show()
```

#### UDTF

UDTF 使用类和 `process()` 方法定义：

```python
@hg.udtf(output_schema=hg.StructType([hg.StructField("word", hg.StringType())]))
class SplitWords:
    def process(self, text: str):
        for word in text.split():
            yield (word,)

session.table("public.docs").select("id", SplitWords("text")).show()
```

> **说明** Hologres Remote UDX 当前不能返回 PostgreSQL `record`，因此 UDTF 的 `output_schema` 必须只有一个字段。FC 和 Hologres 必须在同一地域，并使用 FC 内网 endpoint。

---

## Catalog 与原生 SQL

通过 Session 的 Catalog 方法查看数据库对象：

```python
session.list_schemas()
session.list_tables()
session.list_tables("analytics")
session.current_schema()
session.current_database()
```

执行任意 Hologres SQL（包括 DDL 和 DML）：

```python
session.sql("CREATE TABLE public.t (id bigint)").collect()
session.sql("INSERT INTO public.t VALUES (1)").collect()
```

> **说明** `session.sql()` 本身是惰性的，即使传入 DDL 或 DML，也必须调用 Action（如 `collect()`）才会执行。

---

## 数据类型映射

DataFrame API 提供的 Python 类型与 Hologres/PostgreSQL 类型的对应关系如下：

| Python API | Hologres/PostgreSQL 类型 |
| --- | --- |
| `BooleanType()` | `boolean` |
| `ByteType()` | `"char"` |
| `ShortType()` | `smallint` |
| `IntegerType()` | `integer` |
| `LongType()` | `bigint` |
| `FloatType()` | `real` |
| `DoubleType()` | `double precision` |
| `DecimalType(38, 2)` | `numeric(38,2)` |
| `StringType()` | `text` |
| `StringType(64)` | `varchar(64)` |
| `BinaryType()` | `bytea` |
| `DateType()` | `date` |
| `TimeType()` | `time` |
| `TimestampType()` | `timestamp` |
| `TimestampType(TimestampTimeZone.TZ)` | `timestamptz` |
| `JsonbType()` | `jsonb` |
| `GeographyType()` / `GeometryType()` | PostGIS 类型 |
| `ArrayType(StringType())` | `text[]` |
| `VectorType(float, 768)` | `vector(768)` |

> **说明** `StructType` 和 `StructField` 用于描述 DataFrame 行结构、文件 Schema 和 UDTF 输出结构，不是 Hologres 单列类型。

---

## 异常处理

所有公开异常都继承自 `HoloDataFrameError`：

```python
import hologres.dataframe as hg

try:
    session.table("public.orders").write(
        "public.target",
        overwrite=True,
        on_conflict=("update", ["id"]),
    )
except hg.InvalidArgumentError as exc:
    print(f"参数错误: {exc}")
except hg.UnsupportedOperationError as exc:
    print(f"当前 Hologres 版本不支持: {exc}")
except hg.ExecutionError as exc:
    print(f"Hologres 执行失败: {exc}")
```

异常类型说明：

| 异常 | 说明 |
| --- | --- |
| `InvalidArgumentError` | 参数类型、取值或接口组合不合法。 |
| `CompilationError` | 逻辑计划无法编译为 SQL。 |
| `UnsupportedOperationError` | 当前引擎版本或能力不支持该操作。 |
| `ExecutionError` | 连接、SQL 执行或服务端返回错误。 |
