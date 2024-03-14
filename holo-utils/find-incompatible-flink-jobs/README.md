# find-incompatible-flink-jobs

## 功能介绍
find-incompatible-flink-jobs 是基于[阿里云开发者工具套件](https://help.aliyun.com/zh/flink/developer-reference/getting-started-with-alibaba-cloud-sdk-for-java) 实现的一个小工具，
在升级Hologres版本之前，可以使用本工具扫描指定的实时计算Flink版项目空间中可能不兼容的作业，提前进行版本升级或参数的调整。

## 要求
- 具备superuser权限的账号，或其他能访问实时计算Flink版项目空间的账号
- 且有JAVA环境的机器
- 此工具只支持扫描sql作业，无法扫描jar作业以及不使用Hints方式指定参数的catalog作业

### 命令行参数

可以在find-incompatible-flink-jobs目录下自行编译jar包：
```bash
mvn clean package
```
或者直接下载[jar包](https://github.com/aliyun/alibabacloud-hologres-connectors/releases/download/release-1.4.0/find-incompatible-flink-jobs-1.0-SNAPSHOT-jar-with-dependencies.jar)，指定jar包目录运行如下命令：
```bash
java -cp find-incompatible-flink-jobs-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.hologres.FindIncompatibleFlinkJobs <region> <url> <AccessKeyId> <AccessKeySecret> <binlog/rpc>
```
共有5个参数，含义是：
 1. region: 目标实时计算Flink版项目空间所在的地域名，取值详见下方列表
 2. url: 目标实时计算Flink版项目任意一个作业的连接地址
 3. AccessKeyId: 能访问实时计算Flink版项目空间的账号Id
 4. AccessKeySecret: 能访问实时计算Flink版项目空间的账号Secret
 5. binlog/rpc: 需要检查的作业内容，取值为binlog或者rpc
    * binlog表示检查整个项目中所有作业的hologres binlog源表
    * rpc表示检查整个项目中所有作业使用了rpc模式的维表或结果表

比如，我的实时计算Flink版本所在的region是北京，检查整个项目空间中是否有需要调整的不兼容的hologres binlog源表
```bash
java -cp find-incompatible-flink-jobs-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.hologres.FindIncompatibleFlinkJobs cn-beijing https://vvp.console.aliyun.com/web/aaa/zh/#/workspaces/my-workspace/namespaces/my-namespace/operations my-access-key-id my-access-key-secret binlog
```
运行结果可能如下：
```bash
--------------------以下是版本还小于8.0.5，也没有设置sdkmode = jdbc的hologres binlog 源表-------------------
-------------------------------------------------------------------------------------------------------------
deploymentName                       version                              tableName
read_binlog_deployment_001 vvr-8.0.1-flink-1.17 order_source_table
```
之后便可以根据输出结果，对相应作业的版本进行调整，建议升级到8.0.5以上版本。

#### region参数取值如下，请根据自己项目空间所在地域选择合适的取值

```
cn-hangzhou
cn-shanghai
cn-beijing
cn-zhangjiakou
cn-shenzhen
cn-hongkong
cn-shanghai-finance-1
ap-northeast-1
ap-southeast-1
ap-southeast-3
ap-southeast-5
ap-south-1
eu-central-1
us-east-1
us-west-1
```