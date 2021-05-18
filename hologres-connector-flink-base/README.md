
## Hologres Flink Connector的通用核心代码

无法直接使用，编译安装至本地作为 ***hologres-connector-flink-1.11*** 以及 ***hologres-connector-flink-1.12*** 的依赖

### hologres-connector-flink-base编译

- 结合使用环境的hive版本进行打包，使用-P指定版本参数

- 支持版本如下表

|参数|支持版本|
|:---:|:---:|
|flink版本|flink1.11 <br> flink1.12|

使用-p参数指定flink版本进行编译，如flink1.11可以使用如下命令：

```
mvn install -pl hologres-connector-flink-base clean package -DskipTests -Pflink-1.11
```
