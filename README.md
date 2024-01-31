# hologres-connectors
Connectors for Hologres
* 文档与版本相关，可以选择相应Branch查看对应版本的文档。

# 模块介绍

* [holo-chatbot](./holo-chatbot/)

    使用Hologres向量引擎定制GPT大模型聊天机器人，钉钉机器人版

* [holo-chatbot-webui](./holo-chatbot-webui/)

    使用Hologres+PAI定制大模型聊天机器人，网页版

* [holo-llm](./holo-llm/)

    使用Hologres向量引擎和PAI-LLM服务搭建聊天机器人<br/><br/>

* [holo-client](./holo-client)

    holo-client源码以及使用holo-client读写Hologres的文档
* [holo-client-c](./holo-client-c)
    
    介绍如何通过holo-client-c读写Hologres
* [holo-client-go](./holo-client-go)

    介绍golang使用holo-client-c读写Hologres<br/><br/>

* [holo-e2e-performance-tool](./holo-e2e-performance-tool)

  holo-e2e-performance-tool 是Hologres在数据写入、数据更新、点查场景的性能测试工具。<br/><br/>
    
* [holo-shipper](./holo-shipper)

  holo-shipper 是支持将Holo Instance的部分表导入导出的备份工具。可以在Holo之间搬迁表，也可以dump到中间存储然后再恢复。<br/><br/>

* [holo-utils](./holo-utils)

  一些方便使用或者排查问题的小工具，目前包括[find-incompatible-flink-jobs](./holo-utils/find-incompatible-flink-jobs)工具，用于检测阿里云实时计算项目中可能存在，在升级Hologres版本时不兼容的作业<br/><br/>

* [hologres-connector-examples](hologres-connector-examples)
  
    该模块提供了若干使用该项目下Connector的各种实例代码<br/><br/>

* [hologres-connector-flink-base](./hologres-connector-flink-base)<br/>
  该模块实现了Hologres Flink Connector的通用核心代码

* [hologres-connector-flink-1.11](https://github.com/aliyun/alibabacloud-hologres-connectors/tree/flink-1.11/1.12/hologres-connector-flink-1.11)<br/>
  依赖hologres-connector-flink-base，实现了Flink 1.11版本的Connector

* [hologres-connector-flink-1.12](https://github.com/aliyun/alibabacloud-hologres-connectors/tree/flink-1.11/1.12/hologres-connector-flink-1.12)<br/>
  依赖hologres-connector-flink-base，实现了Flink 1.12版本的Connector，相较于1.11，主要新增了维表场景一对多的实现

* [hologres-connector-flink-1.13](https://github.com/aliyun/alibabacloud-hologres-connectors/tree/flink-1.13/1.14/hologres-connector-flink-1.13)<br/>
  依赖hologres-connector-flink-base，实现了Flink 1.13版本的Connector, 相较于1.12，支持消费holo源表

* [hologres-connector-flink-1.14](https://github.com/aliyun/alibabacloud-hologres-connectors/tree/flink-1.13/1.14/hologres-connector-flink-1.14)<br/>
  依赖hologres-connector-flink-base，实现了Flink 1.14版本的Connector

* [hologres-connector-flink-1.15](./hologres-connector-flink-1.15)<br/>
  依赖hologres-connector-flink-base，实现了Flink 1.15版本的Connector

* [hologres-connector-flink-1.17](./hologres-connector-flink-1.17)<br/>
  依赖hologres-connector-flink-base，实现了Flink 1.17版本的Connector<br/><br/>

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


* [hologres-connector-kafka](./hologres-connector-kafka)

    实现了kafka的Connector<br/><br/>


* [hologres-connector-datax-writer](./hologres-connector-datax-writer)

    依赖[DataX框架](https://github.com/alibaba/DataX)，实现了写hologres插件<br/><br/>

# 使用
目前connector的Release版本已经发布到maven中央仓库，详见[hologres-connectors](https://search.maven.org/search?q=com.alibaba.hologres)

可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。
```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-flink-1.13</artifactId>
    <version>1.4.0</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```

# 下载
也可以在 GitHub Releases 中可以下载最新SNAPSHOT版本已经编译好的jar包

# 编译
各connector会依赖父项目的pom文件，在根目录执行以下命令进行install

```
mvn clean install -N
```