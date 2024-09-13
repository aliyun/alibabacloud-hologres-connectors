
## Hologres Hive Connector的通用核心代码

无法直接使用，编译安装至本地作为 ***hologres-connector-hive-2.x*** 以及 ***hologres-connector-hive-3.x*** 的依赖

### hologres-connector-hive-base编译

- 结合使用环境的hive版本进行打包，使用-P指定版本参数

- 支持版本如下表

|参数|支持版本|
|:---:|:---:|
|hive版本|hive-2 <br> hive-3|

例如使用的hive环境为hive-2.3.8，可以使用如下命令：

```
mvn install -pl hologres-connector-hive-base clean package -DskipTests -Phive-2
```
