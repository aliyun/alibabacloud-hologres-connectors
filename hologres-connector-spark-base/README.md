## Hologres Spark Connector的通用核心代码

无法直接使用，编译安装至本地作为 ***hologres-connector-spark-2.x*** 以及 ***hologres-connector-spark-3.x*** 的依赖

### hologres-connector-spark-base编译

- 结合使用环境的scala及spark版本进行打包，使用-P指定版本参数

- 支持版本如下表

|   参数    |                    支持版本                    |
|:-------:|:------------------------------------------:|
| scala版本 | scala-2.11 <br> scala-2.12 <br> scala-2.13 |
| spark版本 |            spark-2 <br> spark-3            |

例如使用的是scala2.12编译的spark 3.3，可以使用如下命令：

```
mvn install -pl hologres-connector-spark-base clean package -DskipTests -Pscala-2.12 -Pspark-3
```

打包结果名称为 hologres-connector-spark-base_2.12_spark3-1.5.6-SNAPSHOT.jar
