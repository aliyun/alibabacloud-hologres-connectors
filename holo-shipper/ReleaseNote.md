# holo-shipper release notes

## v1.2.2 release note
   - v1.2.2下载地址：
    https://github.com/aliyun/alibabacloud-hologres-connectors/releases/download/release-1.3.0/holo-shipper-1.2.2.jar
### 更新说明     
   - 新增 --allow-table-exists参数，开启时，允许目标表存在
   - 新增 --max-task-num参数，可指定最大可同时运行的任务数
   - 新增 --disable-shard-copy 可禁止分shard的导入方式
   - 以上参数具体说明参见：https://github.com/aliyun/alibabacloud-hologres-connectors/blob/master/holo-shipper/README.md
   - 修复导入/导出执行失败，但holoshipper日志无报错的问题
   
## v1.2.1 release note
   - v1.2.1下载地址：
     https://github.com/aliyun/alibabacloud-hologres-connectors/releases/download/1.1-SNAPSHOT/holo-shipper-1.2.1.jar
###更新说明     
   - 新增 tgMapping 可以指定源tablegroup和目标tablegroup的映射关系。 用法参见：https://github.com/aliyun/alibabacloud-hologres-connectors/blob/master/holo-shipper/README.md
   - 修复源表ddl的table properties内同时含有colocate_with 和 table_group_name时，无法在目标库建表的问题，会忽略源表的colocate_with。

