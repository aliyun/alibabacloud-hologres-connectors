
## 依赖hologres-connector-flink-base，实现了Flink 1.12版本的Connector

### 准备工作
- 需要**Hologres 0.9**及以上版本。
- 需要FLink1.12
- build base jar 并 install 到本地maven仓库
  - -P指定相关版本参数

  ```
  mvn install -pl hologres-connector-flink-base clean package -DskipTests -Pflink-1.12
  ```

- build jar

  ```
  mvn install -pl hologres-connector-flink-1.12 clean package -DskipTests
  ```

### 使用示例

见 hologres-connector-examples子项目

### Hologres Flink Connector相关参数说明

见 hologres-connector-flink-base/README.md