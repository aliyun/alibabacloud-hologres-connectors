# hologres-connectors
Connectors for Hologres

# 模块介绍
* [holo-client-docs](./holo-client-docs)

    介绍如何通过holo-client读写Hologres的文档
* [holo-client-c](./holo-client-c)
    
    介绍如何通过holo-client-c读写Hologres
* [holo-client-go](./holo-client-go)

    介绍golang使用holo-client-c读写Hologres
* [hologres-connector-examples](hologres-connector-examples)
  
    该模块提供了若干使用该项目下Connector的各种实例代码<br/><br/>
  

* [hologres-connector-flink-base](./hologres-connector-flink-base)
  
    该模块实现了Hologres Flink Connector的通用核心代码
* [hologres-connector-flink-1.11](./hologres-connector-flink-1.11)
  
    依赖hologres-connector-flink-base，实现了Flink 1.11版本的Connector
* [hologres-connector-flink-1.12](./hologres-connector-flink-1.12)
  
    依赖hologres-connector-flink-base，实现了Flink 1.12版本的Connector，相较于1.11，主要新增了维表场景一对多的实现
* [hologres-connector-flink-1.13](./hologres-connector-flink-1.13)

    依赖hologres-connector-flink-base，实现了Flink 1.13版本的Connector<br/><br/>


* [hologres-connector-hive-base](./hologres-connector-hive-base)

    该模块实现了Hologres Hive Connector的通用核心代码
* [hologres-connector-hive-2.x](./hologres-connector-hive-2.x)

    依赖hologres-connector-hive-base，实现了Hive2.x版本的Connector
* [hologres-connector-hive-3.x](./hologres-connector-hive-3.x)

    依赖hologres-connector-hive-base，实现了Hive3.x版本的Connector<br/><br/>


* [hologres-connector-spark-base](./hologres-connector-spark-base)

    该模块实现了Hologres Spark Connector的通用核心代码
* [hologres-connector-spark-2.x](./hologres-connector-spark-2.x)

    依赖hologres-connector-spark-base，实现了Spark2.x版本的Connector
* [hologres-connector-spark-3.x](./hologres-connector-spark-3.x)

    依赖hologres-connector-spark-base，实现了Spark3.x版本的Connector<br/><br/>

* [hologres-connector-datax-writer](./hologres-connector-datax-writer)

    依赖[DataX框架](https://github.com/alibaba/DataX)，实现了写hologres插件
    
# 编译
在根目录执行
```mvn install -DskipTests``` 即可，各模块的maven依赖，可参考各自的pom.xml文件
