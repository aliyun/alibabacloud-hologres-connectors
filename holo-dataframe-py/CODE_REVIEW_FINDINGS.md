# Hologres DataFrame 代码审查问题清单

本文记录当前 `holo-dataframe-py` 的代码审查结果，覆盖写入安全、查询正确性、版本探测和发布兼容性问题。

## 实施状态

本文中的问题描述保留为修复依据。当前工作区已实施以下修复：

- overwrite 无法确认分区元数据或服务器版本时 fail-closed；
- legacy overwrite 对 `RawSql` 保守物化，并跟随 `PendingSource.resolved`；
- 所有版本统一拒绝覆盖物理分区父表；
- 版本探测执行失败不再污染 Session 缓存；
- Join 嵌套后维护 relation/column 到外层输出列的 lineage；
- `hologres-client` 提升到 `0.1.2` 并提供跨路径 namespace 支持；
- `hologres-dataframe` 修复版本提升到 `0.1.1`，依赖 `hologres-client>=0.1.2`。
- Session 数据库/schema 切换不再污染旧连接或丢失旧资源登记；未限定表名跟随当前 schema。
- `DataFrame.__repr__()` 不再解析未完成的 `PendingSource`。
- join/union 构造阶段不再触发 schema describe 或远程函数部署。
- `PendingSource` 在首次 action 解析前声明并检查 materialization capability。

对应回归测试和构建校验结果应以提交时的测试记录为准。

## 结论摘要

这份审查意见整体合理，不是误报。

| 编号 | 问题 | 严重级别 | 结论 |
| --- | --- | --- | --- |
| 1 | 分区元数据查询失败后 fail-open，可能把分区覆盖变成全表覆盖 | Blocker | 已修复，待发布 |
| 2 | Legacy overwrite 无法识别 `RawSql` 等自引用，可能清空目标表后再读取 | Blocker | 已修复，待发布 |
| 3 | 版本探测失败被缓存为老版本，可能永久进入 destructive fallback | High / Blocker | 已修复，待发布 |
| 4 | 老版本可能对物理分区父表执行 `TRUNCATE` | Blocker | 已修复，待发布 |
| 5 | Join 嵌套后 relation qualifier 丢失，可能静默返回错误结果 | Blocker | 已修复，待发布 |
| 6 | `hologres-client` 发布包与 dataframe 的 namespace 设计不一致 | Release / Compatibility | 源码已修复，待发布 client |

其中 1、2、4、5 会直接影响数据安全或查询结果，不能通过修改测试预期来关闭。

---

## 1. 分区元数据失败可能导致全表覆盖

### 相关位置

- `hologres/dataframe/_write.py:61-67`
- `hologres/dataframe/_write.py:181-270`
- `hologres/dataframe/_write.py:306-338`
- `tests/test_write.py:248-251`

### 触发方式

```python
df.write("public.target", overwrite=True)
```

`write_dataframe()` 会设置：

```python
need_partition = overwrite or partition is not None
```

因此会查询 Hologres 表属性：

```sql
SELECT property_key, property_value
FROM hologres.hg_table_properties
WHERE table_namespace = ... AND table_name = ...
```

### 当前控制流

`_load_target_metadata()` 捕获所有 `ExecutionError`：

```python
try:
    rows, _ = session.execute(...)
    table_properties = rows
except ExecutionError:
    table_properties = None
```

这里的异常可能来自：

- catalog 查询权限不足；
- catalog 在当前实例版本不存在；
- 查询超时；
- 连接断开或重置；
- Hologres 临时服务异常。

查询失败后，代码保留默认值：

```python
logical_partition_keys = ()
```

随后 `_validate_partition()` 执行：

```python
if partition is None:
    if overwrite and target.logical_partition_keys:
        raise UnsupportedOperationError(...)
    return None
```

因为 `logical_partition_keys` 为空，代码把“无法确认是否为逻辑分区表”解释为“确认不是逻辑分区表”，允许继续执行。

### 可能生成的 SQL

新版本路径：

```sql
INSERT OVERWRITE "public"."target" ("id", "name")
SELECT ...
```

老版本路径：

```sql
TRUNCATE TABLE "public"."target";
INSERT INTO "public"."target" ("id", "name")
SELECT ...;
```

### 风险

如果目标实际是逻辑分区表，调用方忘记传 `partition=` 时本应拒绝操作。当前实现可能因为一次元数据失败而覆盖全部分区。

这是危险的 fail-open：

```text
无法确认目标表分区属性
        ↓
默认按非分区表处理
        ↓
允许全表 overwrite
```

安全行为应为 fail-closed：

```text
overwrite=True 且分区元数据未知
        ↓
拒绝执行
```

### 测试缺口

当前测试：

```python
def test_unknown_binlog_property_does_not_block_server_validation(self):
    session = WriteSession(binlog_query_error=True)
    _table_df(session).write("public.target", overwrite=True)
    assert session.executed[-1].startswith("INSERT OVERWRITE")
```

该测试把属性查询失败视为可以继续执行，实际上固定了危险行为。分区元数据和 Binlog 元数据需要拆开测试：

- 分区元数据未知时，overwrite 必须失败；
- 仅 Binlog 状态未知时，根据单独的 Binlog 安全策略决定是否允许。

### 建议修复

1. 分离分区元数据和 Binlog 元数据的状态。
2. `overwrite=True` 时，分区元数据查询失败必须抛出明确异常。
3. 不要把权限错误、catalog 不存在和瞬时网络错误都转换为空属性。
4. 添加权限失败、执行失败和未知分区状态的回归测试。

---

## 2. Legacy overwrite 自引用可能静默清空目标表

### 相关位置

- `hologres/dataframe/_write.py:125-133`
- `hologres/dataframe/_write.py:433-493`
- `hologres/dataframe/_write.py:540-545`
- `hologres/dataframe/_plan.py:80-95`
- `hologres/dataframe/_plan.py:177-204`

### 背景

Hologres 3.1 以下走 legacy overwrite：

```sql
TRUNCATE TABLE target;
INSERT INTO target SELECT ...;
```

如果 source 引用了 target，必须先物化 source：

```sql
CREATE TEMP TABLE temp AS SELECT ... FROM target;
TRUNCATE TABLE target;
INSERT INTO target SELECT ... FROM temp;
```

### 当前检测逻辑

```python
def _plan_references_table(plan, target):
    return any(
        isinstance(node, _plan.TableScan) and node.table_name == target
        for node in _plan.iter_nodes(plan)
    )
```

它只能识别显式 `TableScan`，例如：

```python
session.table("public.target")
```

### RawSql 遗漏

以下 DataFrame 使用 `RawSql`：

```python
df = session.sql("SELECT id, name FROM public.target")
df.write("public.target", overwrite=True)
```

`RawSql.sql_text` 被视为不透明 SQL，`iter_nodes()` 不解析其中的表引用，因此：

```text
_plan_references_table(...) == False
```

### 已复现 SQL

```sql
TRUNCATE TABLE "public"."target";
INSERT INTO "public"."target" ("id", "name")
SELECT id, name FROM public.target;
```

第二条语句读取的是已经被第一条语句清空的表。最终表现为：

```text
目标表变空
事务提交成功
write() 返回成功
```

这是静默数据丢失。

### PendingSource.resolved 遗漏

`PendingSource` 在 action 阶段可能解析为真实 source：

```python
class PendingSource(LogicalPlan):
    __slots__ = ("loader", "schema", "resolved")
```

`iter_nodes()` 只遍历 `child`、`left`、`right`，不会跟随 `resolved`。即使 `resolved` 是目标表的 `TableScan`，也可能检测不到。

### 建议修复

最稳妥的方案是：legacy overwrite 在任何 `TRUNCATE` 或 `DELETE` 前统一物化 source query。

如果仍保留引用检测，至少需要：

1. 跟随 `PendingSource.resolved`；
2. 对 `RawSql` 采用保守策略，无法可靠分析时先物化；
3. 不使用简单字符串包含判断解析 SQL，因为存在注释、别名、嵌套查询、字符串字面量和不同限定方式；
4. 在任何 destructive statement 前完成 source 物化。

### 必须增加的测试

- `session.sql("SELECT ... FROM public.target")` 写回同表；
- Raw SQL 使用 schema 限定和别名；
- Raw SQL 包含嵌套查询；
- `PendingSource.resolved` 指向目标表；
- source 不引用目标表时的正常 legacy overwrite。

---

## 3. 版本探测失败会永久进入旧版本路径

### 相关位置

- `hologres/dataframe/session.py:51-53`
- `hologres/dataframe/session.py:1117-1155`
- `hologres/dataframe/_write.py:101-133`
- `hologres/dataframe/_sources.py:441-450`

### 当前行为

`Session.server_version()` 执行：

```sql
SELECT hg_version()
```

如果执行失败：

```python
except ExecutionError:
    self._server_version = (0, 0, 0)
    return self._server_version
```

一次临时超时、连接重置或服务异常会被永久缓存为“服务器版本 0.0.0”。

### 对 overwrite 的影响

```python
if session.server_version() >= (3, 1, 0):
    # native INSERT OVERWRITE
else:
    # legacy TRUNCATE/DELETE + INSERT
```

风险链路：

```text
版本探测临时失败
        ↓
缓存 (0, 0, 0)
        ↓
永久选择 legacy overwrite
        ↓
执行 TRUNCATE/DELETE + INSERT
```

这会激活并放大问题 2 和问题 4。

### 对其他能力的影响

同一个缓存还可能错误禁用或拒绝：

- stage 上传路径；
- `external_files`；
- object table；
- `DROP ... FORCE`；
- 其他按版本门控的能力。

### 版本解析问题

当前正则：

```python
_VERSION_RE = re.compile(r"Hologres\s+(\d+)\.(\d+)\.(\d+)")
```

它接受：

```text
Hologres 4.1.0
```

但拒绝：

```text
Hologres 4.1
hologres 4.1.0
```

正式服务通常返回三个数字段，所以正则是健壮性问题；把所有 `ExecutionError` 缓存成老版本则是确定的安全问题。

### 建议修复

1. 不缓存瞬时探测失败。
2. 区分“已确认老版本”“版本未知”和“探测临时失败”。
3. overwrite 遇到未知版本时拒绝执行，不能自动进入 destructive fallback。
4. 仅对明确可重试错误进行有限重试。
5. 增加“第一次探测失败，下一次可以重试”的测试。
6. 增加“版本未知时不会发送 TRUNCATE/DELETE”的测试。

---

## 4. 老版本可能 TRUNCATE 物理分区父表

### 相关位置

- `hologres/dataframe/_write.py:210-230`
- `hologres/dataframe/_write.py:306-338`
- `hologres/dataframe/_write.py:402-414`
- `hologres/dataframe/_write.py:433-466`

### 目标类型检测

`_load_target_metadata()` 通过 PostgreSQL catalog 判断目标是否为物理分区父表，并设置：

```python
physical_partition_parent = True
```

### 当前 guard 的两个缺陷

第一，`_validate_partition()` 在 `partition=None` 时提前返回：

```python
if partition is None:
    ...
    return None
```

因此不会执行后面的物理分区父表检查。

第二，`_build_overwrite_sql()` 只在新版本检查：

```python
if server_version >= _OVERWRITE_MIN_VERSION and target.physical_partition_parent:
    raise UnsupportedOperationError(...)
```

Hologres 3.1 以下会绕过该 guard。

### 已复现 SQL

旧版本、物理分区父表、`partition=None` 时，代码生成：

```sql
TRUNCATE TABLE "public"."partition_parent";
INSERT INTO "public"."partition_parent" (...)
SELECT ...;
```

### 风险

对物理分区父表执行 `TRUNCATE` 可能影响所有子分区，而不是用户期望处理的一个分区。

“目标是物理分区父表”是表类型约束，不应依赖服务器版本，也不应依赖调用方是否传了 `partition=`。

### 建议修复

在 native/legacy 路径选择之前统一拒绝：

```python
if overwrite and target.physical_partition_parent:
    raise UnsupportedOperationError("write the physical partition child table instead")
```

避免在 `_validate_partition()` 和 SQL builder 中分别实现条件不同的 guard。

### 必须增加的测试

- 3.1+，`partition=None`；
- 3.1 以下，`partition=None`；
- 3.1+，提供 `partition=`；
- 3.1 以下，提供 `partition=`。

所有情况都应在生成 destructive SQL 前失败。

---

## 5. Join 嵌套后列来源丢失

### 相关位置

- `hologres/dataframe/_compiler.py:148-156`
- `hologres/dataframe/_compiler.py:346-371`
- `hologres/dataframe/_compiler.py:405-411`
- `hologres/dataframe/_expr.py:474-526`
- `tests/test_join.py:352-407`
- `tests/test_join.py:464-513`

### 示例

```python
j = people.join(
    staff,
    on=people["id"] == staff["id"],
    lsuffix="_l",
)
```

输出列：

```text
id_l, name_l, id, name
```

列来源实际是：

```text
people.id -> id_l
staff.id  -> id
```

### 嵌套时发生了什么

Join 编译时，relation 会绑定 SQL alias：

```text
people -> t0
staff  -> t1
```

后续操作不能继续融合时，编译器调用 `_nest()`：

```python
def _nest(self, state):
    alias = self._next_alias()
    subquery = sql.SQL("({}) {}").format(...)
    self._relation_alias = {}
    self._ambiguous = set()
    return _SelectState(from_sql=subquery)
```

清除 `t0`、`t1` 是必要的，因为它们不在外层作用域。但代码没有建立“原 relation/列到外层输出列”的 lineage 映射。

### 表达式重写问题

`rewrite_relations()` 对不在 mapping 中的 relation 直接返回：

```python
if key not in mapping:
    return expression
```

该列随后渲染为裸列名。

### 已复现 SQL

```python
j.filter(people["id"] > 1)
```

当前生成：

```sql
SELECT *
FROM (
    SELECT
        "t0"."id" AS "id_l",
        "t0"."name" AS "name_l",
        "t1"."id",
        "t1"."name"
    FROM "public"."people" "t0"
    INNER JOIN "public"."staff" "t1"
        ON ("t0"."id" = "t1"."id")
) "t2"
WHERE ("id" > 1)
```

用户引用的是 `people["id"]`，正确条件应为：

```sql
WHERE ("id_l" > 1)
```

实际条件使用了右表输出的：

```sql
WHERE ("id" > 1)
```

### LEFT JOIN 风险

```python
j = people.join(
    staff,
    on=people["id"] == staff["id"],
    how="left",
    lsuffix="_l",
)
j.filter(people["id"].is_not_null())
```

当前可能生成：

```sql
WHERE ("id" IS NOT NULL)
```

这里的 `id` 是右表列。未匹配行的右表字段为 NULL，因此 LEFT JOIN 的未匹配行被错误删除，语义接近 INNER JOIN。

### 为什么现有测试没有发现

现有嵌套测试使用列名未改变的场景：

```python
joined.select(orders["amount"]).filter(orders["amount"] > 1)
```

裸列 `amount` 仍可能碰巧绑定正确，因此无法覆盖 suffix 后的错误绑定。

### 修复方向

不能简单地让所有 out-of-scope relation 一律报错，因为部分嵌套引用可以合法地重新绑定。

正确方向是维护列 lineage：

```text
原 relation + 原列名
        ↓
当前 SELECT 的输出列名
```

嵌套后：

1. 能唯一映射时，重写到正确外层列，例如 `people.id -> id_l`；
2. 无法唯一映射时，抛出明确编译错误；
3. 不能静默退化为裸列；
4. `hg.col("name")` 这类原本未绑定 relation 的列仍保留裸列语义。

### 必须增加的测试

- suffix 后引用左侧原列；
- suffix 后引用右侧原列；
- LEFT JOIN + `is_not_null()`；
- LEFT JOIN + `is_null()`；
- 双侧同名列同时 suffix；
- 多层嵌套；
- 无法唯一映射时必须抛异常。

---

## 6. 发布包与 namespace 设计不一致

### 当前状态

`holo-dataframe-py/pyproject.toml` 声明：

```toml
"hologres-client>=0.1.2.dev0"
```

当前仓库中的 client 源码版本是：

```python
__version__ = "0.1.1"
```

PyPI 上存在 `hologres-client==0.1.2.dev0`，但该 wheel 的 `hologres/__init__.py` 不包含当前分支源码中的：

```python
__path__ = __import__("pkgutil").extend_path(__path__, __name__)
```

### 实际影响

`hologres-client` 和 `hologres-dataframe` 共用顶级包：

```text
hologres
├── client modules
└── dataframe
```

普通 pip 在同一个虚拟环境安装时，两个 distribution 通常落入同一个 `site-packages/hologres` 目录，因此标准安装已经实测可以导入：

```text
hologres-dataframe==0.1.0
hologres-client==0.1.2.dev0
```

所以“普通 `pip install hologres-dataframe` 必然失败”这个说法过强。

但是分离路径场景会失败，例如：

- 使用 `pip install --target` 分开安装；
- 一个包在用户 site-packages，另一个在系统 site-packages；
- vendor 或插件系统把 distribution 放在不同目录；
- 其他多 `sys.path` 根目录部署。

已复现结果：

```text
hologres_path = [...单一路径...]
ModuleNotFoundError: No module named 'hologres.dataframe'
```

### 测试缺口

当前测试：

```python
assert len(hologres.__path__) >= 1
assert hasattr(hologres, "dataframe")
```

`len(...) >= 1` 是必然成立的，不能证明 namespace 可以跨两个路径工作。editable install 也可能通过 finder 掩盖真实 wheel 布局问题。

### 建议修复

方案 A：

1. 发布包含 `extend_path` 的新 `hologres-client`；
2. 使用新的明确版本号；
3. dataframe 依赖约束指向该版本；
4. 增加两个独立安装目录的导入测试。

方案 B：

重新设计为标准 namespace package，并同步调整 setuptools 包发现和公开 API 布局。

无论采用哪种方案，都应使用实际 wheel 在两个不同目录安装，然后验证：

```python
import hologres
import hologres.dataframe
```

---

## 建议修复顺序

1. 修复 1、2、4：先消除全表清空和物理分区数据风险。
2. 修复 3：版本未知时禁止 destructive fallback。
3. 修复 5：维护嵌套列 lineage，防止静默错误结果。
4. 修复 6：发布包含 namespace 修复的 client，并调整依赖约束。

## 发布判断

`hologres-dataframe==0.1.0` 仍包含上述问题，不建议继续扩大使用范围。
当前修复计划通过 `hologres-client==0.1.2` 和
`hologres-dataframe==0.1.1` 发布。

这些问题可能导致：

- 覆盖逻辑分区表的全部分区；
- 自引用 overwrite 后目标表静默变空；
- 清空物理分区父表下的数据；
- Join 返回错误结果但不报告异常。

它们属于稳定版本必须解决的问题，不是一般代码风格或测试完善项。
