# Spark 3.x版本的Hologres Connector
Spark是用于大规模数据处理的统一分析引擎，Hologres已经与Spark（社区版以及EMR Spark版）高效打通，快速助力企业搭建数据仓库。Hologres提供的Spark Connector，支持在Spark集群创建Hologres Catalog，以外表的方式进行高性能批量读取和导入，相比原生 JDBC 有更好的性能。



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
  mvn install -pl hologres-connector-spark-base clean package -DskipTests -Pscala-2.12 -Pspark-3
  ```

打包结果名称为 hologres-connector-spark-3.x-1.6.0-SNAPSHOT-jar-with-dependencies.jar

#### build jar

  ```
  mvn -pl hologres-connector-spark-3.x clean package -DskipTests
  ```
## 下载Release包

+ Spark 读写 Hologres时需要引用connector的JAR包，最新的依赖可以从[maven中央仓库](https://central.sonatype.com/artifact/com.alibaba.hologres/hologres-connector-spark-3.x)下载，在项目中使用可以参照如下pom文件进行配置。

```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-spark-3.x</artifactId>
    <version>1.5.6</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```

