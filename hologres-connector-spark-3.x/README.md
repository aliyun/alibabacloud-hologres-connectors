# Spark 3.x版本的Hologres Connector

Spark是用于大规模数据处理的统一分析引擎，Hologres已经与Spark（社区版以及EMR Spark版）高效打通，快速助力企业搭建数据仓库。Hologres提供的Spark
Connector，支持在Spark集群创建Hologres Catalog，以外表的方式进行高性能批量读取和导入，相比原生 JDBC 有更好的性能。

## 使用文档

详细使用文档请参考[Spark读写Hologres](https://help.aliyun.com/zh/hologres/user-guide/spark-read-and-write-hologres)

## 项目编译

connector依赖父项目的pom文件，在本项目根目录执行以下命令进行install

```
mvn clean install -N
```

#### build base jar 并 install 到本地maven仓库

- -P指定相关版本参数，本项目使用scala2.12以及spark3.3.1，详情请查看hologres-connector-spark-base子项目README

  ```
  mvn install package -pl hologres-connector-spark-base -DskipTests -Pscala-2.12 -Pspark-3
  ```

打包结果名称为 hologres-connector-spark-3.x-1.6.2-jar-with-dependencies.jar

#### build jar

  ```
  mvn package -pl hologres-connector-spark-3.x -DskipTests
  ```

## 下载Release包

+ Spark 读写
  Hologres时需要引用connector的JAR包，最新的依赖可以从[maven中央仓库](https://central.sonatype.com/artifact/com.alibaba.hologres/hologres-connector-spark-3.x)
  下载，在项目中使用可以参照如下pom文件进行配置。

```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-spark-3.x</artifactId>
    <version>1.6.2</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```

## v1.6.x 新功能与修复

### 新功能

#### Stage 写入模式

新增 `write.mode=stage`，通过 Hologres Stage 中转数据进行批量写入，适合对写入事务性有要求的场景。写入流程为：先将数据写入 Hologres Stage（类似对象存储的临时目录），再由 Hologres 将 Stage 数据加载到目标表。

相关参数：

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| write.stage.batch_size | stage 写入时每批次的行数 | 8192 |
| write.stage.file_size | stage 写入时每个文件的最大字节数 | 67108864（64MB） |
| write.stage.only_stage | 是否只写入 Stage，不将数据加载到目标表（可用于数据预暂存） | false |
| write.stage.ttl | Stage 数据的 TTL（秒），仅 `write.stage.only_stage=true` 时生效 | 7200 |
| write.stage.compression | 是否开启 Arrow LZ4 压缩，减少 Stage 数据传输量（需 holo-client >= 2.7.6） | false |

#### 逻辑分区表写入

支持向 Hologres 逻辑分区表（LOGICAL PARTITION BY LIST）的指定子表写入数据。写入时通过配置目标分区列和分区值，Connector 会自动生成对应的 `PARTITION` 子句。

相关参数：

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| write.target_partition_columns | 目标分区列名，多个列以逗号分隔，列名需用双引号包裹（如 `"ds"` 或 `"ds","kind"`） | 无 |
| write.target_partition_values | 目标分区值，多列间以逗号分隔，多分区以分号分隔（如 `"20250101"` 或 `"20250101","100";"20250102","200"`） | 无 |

#### 读取分片策略

新增 `read.split.strategy` 参数，支持对内表、外表和 View 进行并行分片读取，提升大数据量读取性能。

支持三种策略：
- `shard`（默认）：按 Hologres 数据分片（shard）进行并行读取，适合内表
- `range`：按指定列的值范围均匀划分分片，适合数值或日期类型列、以及外表/View
- `partition`：按表的分区键进行分片读取，适合分区表

相关参数：

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| read.split.strategy | 分片策略：`shard` / `range` / `partition` | shard |
| read.split.column | `range` 策略时的分片列（数值或日期类型） | 无 |
| read.split.lower_bound | `range` 策略时的分片下界 | 无 |
| read.split.upper_bound | `range` 策略时的分片上界 | 无 |
| read.split.num | 目标分片数量 | 80 |

#### 写入限速

新增 `write.rps_limit` 参数，限制写入速度（单位：records per second），防止写入速度过快对 Hologres 实例造成过大压力。

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| write.rps_limit | 限制写入速度（条/秒），不配置则不限速 | 无 |

### Bug 修复

- **修复 Arrow 读取模式不支持 jsonb 类型的问题**：使用 `read.mode=bulk_read`（Arrow 格式）读取时，现在可以正确读取 `jsonb` 类型的字段。
- **修复写入 1970 年之前日期数据时报错的问题**：修复了写入早于 1970-01-01 的 `date` 类型数据时因类型转换错误导致写入失败的问题。

