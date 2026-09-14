# hologres-connector-spark

统一的 Hologres Spark Connector 模块，通过 Maven profile 切换 Spark 3.x / 4.x 编译。

## 构建环境要求

| Profile | Scala | Spark | JDK |
|---------|-------|-------|-----|
| 默认 (spark-3) | 2.12.21 | 3.5.3 | 11+ |
| `-Pscala-2.13,spark-4` | 2.13.17 | 4.1.3 | 17+ |

> **注意**：Scala 2.12 与 2.13 二进制不兼容，切换 profile 后务必先执行 `mvn clean`。

## 打包

```bash
# Spark 3.x（默认）
mvn clean package -pl hologres-connector-spark -am -DskipTests

# Spark 4.x（需 JDK 17+）
JAVA_HOME=/path/to/jdk17 mvn clean package -pl hologres-connector-spark -am -Pscala-2.13,spark-4 -DskipTests
```

打包结果：

- Spark 3.5.3：`hologres-connector-spark-3.5.3-<version>-jar-with-dependencies.jar`
- Spark 4.1.3：`hologres-connector-spark-4.1.3-<version>-jar-with-dependencies.jar`

源码共用同一模块，但 Spark 3.x（Scala 2.12）与 Spark 4.x（Scala 2.13）的二进制制品不能混用。

## Maven 依赖

根据运行时 Spark 版本选择对应制品，两者版本号保持一致。

```xml
<!-- Spark 3.x -->
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-spark-3.5.3</artifactId>
    <version>1.6.3-SNAPSHOT</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>

<!-- Spark 4.x -->
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-spark-4.1.3</artifactId>
    <version>1.6.3-SNAPSHOT</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```

## 包路径说明

核心实现位于中性包 `com.alibaba.hologres.spark`：

- `com.alibaba.hologres.spark.SourceProvider` — DataSource 入口
- `com.alibaba.hologres.spark.HoloTableCatalog` — Spark Catalog 实现
- `com.alibaba.hologres.spark.HoloTable` — Table 实现

### 向后兼容别名

历史包路径通过别名类保留，已有配置无需修改：

| 旧路径 | 新路径 |
|--------|--------|
| `com.alibaba.hologres.spark3.SourceProvider` | `com.alibaba.hologres.spark.SourceProvider` |
| `com.alibaba.hologres.spark3.HoloTableCatalog` | `com.alibaba.hologres.spark.HoloTableCatalog` |
