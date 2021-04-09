# build jar
  
- 结合使用环境的scala及spark版本进行打包，使用-P指定版本参数

- 支持版本如下表

|参数|支持版本|
|:---:|:---:|
|scala版本|scala-2.11 <br> scala-2.12 <br> scala-2.13|
|spark版本|spark-2 <br> spark-3|
  



例如使用的是scala2.11编译的spark2.4，可以使用如下命令：

```
mvn install -pl hologres-connector-spark-base clean package -DskipTests -Pscala-2.11 -Pspark-2
```
打包结果名称为 hologres-connector-spark-base_2.11_spark2-1.0-SNAPSHOT.jar
