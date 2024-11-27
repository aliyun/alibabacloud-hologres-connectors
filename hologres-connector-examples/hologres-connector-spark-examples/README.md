# Spark-connector-Examples

在Examples模块下，有如下几个示例：
* 1.SparkHoloTableCatalogExample

  通过Holo Spark connector, 创建Hologres Catalog进行读写的示例

* 2.SparkWriteDataFrameToHoloExample

  一个使用java实现的通过Holo Spark connector将数据写入至Hologres的应用
  使用scala脚本实现的例子可以参考 hologres-connector-spark-3.x/README.md

* 3.SparkReadHoloToDataFrameExample

  一个使用java实现的通过Holo Spark connector从Hologres读取数据的应用
  使用scala脚本实现的例子可以参考 hologres-connector-spark-3.x/README.md

* 4.SparkToHoloRepartitionExample

  一个使用scala实现的通过Holo Spark connector将数据根据holo的distribution key进行repartition，从而实现高性能的批量导入holo有主键表的应用



## 提交Spark作业
当前的Spark example默认使用Spark 3.3版本，测试的时候请使用Spark 3.3版本集群

###  编译
在本项目(hologres-connector-spark-examples)根目录运行```mvn package -DskipTests```

在spark集群通过spark-submit提交作业并指定参数即可,以 SparkWriteDataFrameToHoloExample为例:
```
spark-submit --class com.alibaba.hologres.spark.example.SparkWriteDataFrameToHoloExample --jars target/hologres-connector-spark-examples-1.0.0-jar-with-dependencies.jar --endpoint ${ip:port} --username ${user_name} --password ${password} --database {database} --tablename sink_table
```
## 在IDEA中运行和调试

以上是针对提交作业到Spark集群的情况，用户也可以在IDEA等编辑器中运行代码，需要在运行配置中设置"
将带有provided作用域的依赖项添加到类路径"