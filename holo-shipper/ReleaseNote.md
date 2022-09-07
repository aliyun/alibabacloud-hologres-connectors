# holo-shipper release notes

## v1.2.1 release note
   - v1.2.1下载地址：
     https://github.com/aliyun/alibabacloud-hologres-connectors/releases/download/1.1-SNAPSHOT/holo-shipper-1.2.1.jar
###更新说明     
   - 新增 tgMapping 可以指定源tablegroup和目标tablegroup的映射关系。 用法参见：https://github.com/aliyun/alibabacloud-hologres-connectors/blob/master/holo-shipper/README.md
   - 修复源表ddl的table properties内同时含有colocate_with 和 table_group_name时，无法在目标库建表的问题，会忽略源表的colocate_with。

