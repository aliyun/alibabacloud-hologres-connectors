# 通过holo-client 写入Hologres

## 功能介绍
holo-client适用于大批量数据写入。holo-client基于libpq实现，使用时请确认实例剩余可用连接数。

-查看最大连接数
```sql
show max_connections;
```
- 查看已使用连接数
```sql
select count(*) from pg_stat_activity;
```

## holo-client 引入
引入holo-client的头文件和动态库（libholo-client.so）
或静态库（libholo-client.a）
### 生成动态库
1. 安装libpq log4c jemalloc  
libpq: 
```
yum install postgresql postgresql-devel  
```
或 
```
yum install libpq libpq-dev  
```
log4c:
```
yum install log4c log4c-devel
```
jemalloc: 
```
yum install jemalloc jemalloc-devel
```

2. 在holo-client-c文件夹下
```
mkdir build
cd build
cmake ..
make install
```
头文件在holo-client-c/build/out/include  
动态库和静态库在holo-client-c/build/out/lib

如果编译器find_path或find_library报错，可以尝试修改CMakeLists.txt，手动指定路径为本地依赖的路径，例如：
find_path (LIBPQ_INCLUDE_DIRS NAMES libpq-fe.h REQUIRED)
改为
find_path (LIBPQ_INCLUDE_DIRS NAMES libpq-fe.h HINTS /usr/include/postgresql NO_DEFAULT_PATH REQUIRED)

## 数据写入
建议项目中创建HoloClient单例，通过threadSize控制并发（每并发占用1个连接，空闲超过connectionMaxIdleMs将被自动回收）

### 写入普通表
```c
holo_client_logger_open();
char* connInfo = "host=xxxx.hologres.aliyuncs.com port=xxx dbname=xxxxx user=xxxxxxxxxx password=xxxxxxxxxx";
HoloConfig config = holo_client_new_config(connInfo);

HoloClient* client = holo_client_new_client(config);
//create table schema_name.table_name (id int, name text, address text)
HoloTableSchema* schema = holo_client_get_tableschema(client, "schema_name", "table_name", true);
HoloMutation mutation = holo_client_new_mutation_request(schema);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0", 8);
holo_client_submit(client, mutation);

mutation = holo_client_new_mutation_request(schema);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
holo_client_submit(client, mutation);

holo_client_flush_client(client);

holo_client_close_client(client);
holo_client_logger_close();
```

### 写入分区表
若分区已存在，不论dynamicPartition为何值，写入数据都将插入到正确的分区表中；若分区不存在，dynamicPartition设置为true时，将会自动创建不存在的分区，否则插入失败
```c
holo_client_logger_open();
char* connInfo = "host=xxxx.hologres.aliyuncs.com port=xxx dbname=xxxxx user=xxxxxxxxxx password=xxxxxxxxxx";
HoloConfig config = holo_client_new_config(connInfo);
config.dynamicPartition = true;

HoloClient* client = holo_client_new_client(config);
//create table schema_name.table_name (id int, name text, address text) partition by list(name)
HoloTableSchema* schema = holo_client_get_tableschema(client, "schema_name", "table_name", true);
HoloMutation mutation = holo_client_new_mutation_request(schema);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0", 8);
holo_client_submit(client, mutation);

mutation = holo_client_new_mutation_request(schema);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
holo_client_submit(client, mutation);

holo_client_flush_client(client);

holo_client_close_client(client);
holo_client_logger_close();
```

### 写入含主键表
```c
holo_client_logger_open();
char* connInfo = "host=xxxx.hologres.aliyuncs.com port=xxx dbname=xxxxx user=xxxxxxxxxx password=xxxxxxxxxx";
HoloConfig config = holo_client_new_config(connInfo);
config.writeMode = INSERT_OR_UPDATE;//配置主键冲突时策略

HoloClient* client = holo_client_new_client(config);
//create table schema_name.table_name (id int, name text, address text, primary key(id))
HoloTableSchema* schema = holo_client_get_tableschema(client, "schema_name", "table_name", true);
HoloMutation mutation = holo_client_new_mutation_request(schema);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0", 8);
holo_client_submit(client, mutation);

mutation = holo_client_new_mutation_request(schema);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
holo_client_submit(client, mutation);

holo_client_flush_client(client);

holo_client_close_client(client);
holo_client_logger_close();
```
### 根据主键删除
```c
holo_client_logger_open();
char* connInfo = "host=xxxx.hologres.aliyuncs.com port=xxx dbname=xxxxx user=xxxxxxxxxx password=xxxxxxxxxx";
HoloConfig config = holo_client_new_config(connInfo);

HoloClient* client = holo_client_new_client(config);
//create table schema_name.table_name (id int, name text, address text, primary key(id))
HoloTableSchema* schema = holo_client_get_tableschema(client, "schema_name", "table_name", true);
HoloMutation mutation = holo_client_new_mutation_request(schema);
holo_client_set_request_mode(mutation, DELETE);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
holo_client_submit(client, mutation); //删除必须设置所有的主键

mutation = holo_client_new_mutation_request(schema);
holo_client_set_request_mode(mutation, DELETE);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
holo_client_submit(client, mutation);

holo_client_flush_client(client);

holo_client_close_client(client);
holo_client_logger_close();
```

### 写入失败的处理
```c
holo_client_logger_open();
char* connInfo = "host=xxxx.hologres.aliyuncs.com port=xxx dbname=xxxxx user=xxxxxxxxxx password=xxxxxxxxxx";
HoloConfig config = holo_client_new_config(connInfo);
config.exceptionHandler = handle_failed_record; //每条插入失败的record都会调用这个函数
HoloClient* client = holo_client_new_client(config);

HoloTableSchema* schema = holo_client_get_tableschema(client, "schema_name", "table_name", true);
HoloMutation mutation = holo_client_new_mutation_request(schema);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
holo_client_submit(client, mutation); 

mutation = holo_client_new_mutation_request(schema);
holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
holo_client_submit(client, mutation);

holo_client_flush_client(client);

holo_client_close_client(client);
holo_client_logger_close();
```

### 根据主键点查
```c
holo_client_logger_open();
char* connInfo = "host=xxxx.hologres.aliyuncs.com port=xxx dbname=xxxxx user=xxxxxxxxxx password=xxxxxxxxxx";
HoloConfig config = holo_client_new_config(connInfo);

HoloClient* client = holo_client_new_client(config);
//create table schema_name.table_name (id int, name text, address text, primary key(id))
HoloTableSchema* schema = holo_client_get_tableschema(client, "schema_name", "table_name", true);
HoloGet get = holo_client_new_get_request(schema);
holo_client_set_get_val_with_text_by_colindex(get, 0, "0", 1);

holo_client_get(client, get); //点查必须设置所有的主键

HoloRecord* res = holo_client_get_record(get); //异步得到结果

if (res == NULL) { //查不到记录或者发生异常
    printf("No record\n");
}
else {
    for (int i = 0; i < schema->nColumns; i++) {
        printf("%s\n", holo_client_get_record_val(res,i)); //第i列的结果， text的形式
    }
}

holo_client_destroy_get_request(get); //得到结果后需要手动释放

holo_client_flush_client(client);

holo_client_close_client(client);
holo_client_logger_close();
```

## 附录
### HoloConfig参数说明
| 参数名 | 默认值 | 说明 |
| --- | --- | --- |
| connInfo| 无 | 必填, 格式:“host=xxxxxxxxx port=xxx dbname=xxxxx user=xxxxxxxxxx password=xxxxxxxxxx”| 
| dynamicPartition | false | 若为true，当分区不存在时自动创建分区 |
| useFixedFe | false | 若为true，写入将交给holo的FixedFE执行，会降低写入时的并发连接数 |
| writeMode | INSERT_OR_REPLACE | 当INSERT目标表为有主键的表时采用不同策略<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE当主键冲突时，更新所有列|
| batchSize | 512 | 每个写入线程的最大批次大小，在经过WriteMode合并后的插入数量达到batchSize时进行一次批量提交 |
| threadSize | 1 | 写入并发线程数（每个并发占用1个数据库连接） |
| shardCollectorSize | 2 * threadSize | 每个表的缓冲区个数，建议shardCollectorSize大于threadSize |
| writeBatchByteSize | 2MB | 每个写入线程的最大批次bytes大小，在经过WriteMode合并后的插入数据字节数达到writeBatchByteSize时进行一次批量提交 |
| writeBatchTotalByteSize | writeBatchByteSize * shardCollectorSize | 所有表所有缓冲区所有数据总和最大bytes大小，达到writeBatchTotalByteSize时全部提交 |
| writeMaxIntervalMs | 10000 ms | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交 |
| exceptionHandler | handle_exception_by_doing_nothing | 对于写入失败的数据的处理函数 |
| retryCount | 3 | 当连接故障时的重试次数 |
| retrySleepInitMs | 1000 ms | 每次重试的等待时间 = retrySleepInitMs + retry * retrySleepStepMs |
| retrySleepStepMs | 10 * 1000 ms | 每次重试的等待时间 = retrySleepInitMs + retry * retrySleepStepMs |
| connectionMaxIdleMs| 60000 ms | 写入线程数据库连接的最大Idle时间，超过连接将被释放|


### LOG说明
holo client用的是log库是log4c

在所有跟holo client有关的代码开始前需要调用 holo_client_logger_open();
结束后需要调用 holo_client_logger_close();

*注意

holo_client_logger_open()和holo_client_logger_close()在整个代码里分别只能调用一次，建议在所有和holo client有关的代码的开始前和结束后调用。
将log4crc文件复制到最终生成的可执行文件的同一文件夹下。如果要修改log的level和打印到的地点，就修改log4crc

log4crc example1(打印到stdout):
```xml
<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE log4c SYSTEM "">

<log4c version="1.2.4">

	<config>
		<bufsize>0</bufsize>
		<debug level="2"/>
		<nocleanup>0</nocleanup>
		<reread>1</reread>
	</config>

	<category name="root" priority="notice"/>
        <category name="holo-client" priority="debug" appender="stdout"/>
             <!--priority 及以上的日志会打印
         -->
	
	<appender name="stdout" type="stream" layout="basic"/>
	<appender name="stderr" type="stream" layout="dated"/>
	<appender name="syslog" type="syslog" layout="basic"/>

	<layout name="basic" type="basic"/>
	<layout name="dated" type="dated"/>

</log4c>
```

log4crc example2(打印到文件mylogfile):
```xml
<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE log4c SYSTEM "">

<log4c version="1.2.4">

	<config>
		<bufsize>0</bufsize>
		<debug level="2"/>
		<nocleanup>0</nocleanup>
		<reread>1</reread>
	</config>

	<category name="root" priority="notice"/>
        <category name="holo-client" priority="debug" appender="myrollingfileappender"/>

    <rollingpolicy name="myrollingpolicy" type="sizewin" maxsize="104857600" maxnum="10" />    
     <!--sizewin 表示达到最大值后新建日志文件  值由maxsize设定
            maxnum  最大文件数目 
         -->
 
    <appender name="myrollingfileappender" type="rollingfile" logdir="./" prefix="mylogfile" layout="basic" rollingpolicy="myrollingpolicy" />
    <!--logdir 日志输出路径
           prefix  文件名
           layout 输出格式 （与下方layout对应）
              例如dated为:
                  20110727 09:21:10.167 WARN     log4ctest- [    main.c][  57][      main()]: shit!-99947
                  20110727 09:21:10.167 WARN     log4ctest- [    main.c][  57][      main()]: shit!-99948
                  20110727 09:21:10.167 WARN     log4ctest- [    main.c][  57][      main()]: shit!-99949
              basic为:
                  WARN     log4ctest - [    main.c][  57][      main()]: shit!-99982
                  WARN     log4ctest - [    main.c][  57][      main()]: shit!-99983
                  WARN     log4ctest - [    main.c][  57][      main()]: shit!-99984
                  WARN     log4ctest - [    main.c][  57][      main()]: shit!-99985              
        -->
	
	<appender name="stdout" type="stream" layout="basic"/>
	<appender name="stderr" type="stream" layout="dated"/>
	<appender name="syslog" type="syslog" layout="basic"/>

	<layout name="basic" type="basic"/>
	<layout name="dated" type="dated"/>

</log4c>
```

### 为每一行数据赋值的具体函数
具体函数定义见include/request.h

为每一个mutation(每一行数据赋值)可以用column index或者column name, 但是推荐尽量用column index, 因为column name效率比较低

smallint, int, bigint, bool, float4, float8, text, timestamp, timestamptz, int[], bigint[], bool[], float4[], float8[], text[]类型的列有对应的赋值函数

其他类型的列（包括上面的类型）都可以用 holo_client_set_req_val_with_text_by_colindex 和 holo_client_set_req_val_with_text_by_colname 以字符串的形式插入, 所有以字符串形式的插入必须在函数参数中指定插入字符串的长度（不包含\0）

| holo类型 | 赋值函数 | 说明 |<br>
| --- | --- | --- |<br>
| smallint| int holo_client_set_req_int16_val_by_colindex(HoloMutation mutation, int colIndex, int16_t value)<br>int holo_client_set_req_int16_val_by_colname(HoloMutation mutation, const char* colname, int16_t value)<br>int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len)<br>int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) | 成功则返回0 |<br>
| integer | int holo_client_set_req_int32_val_by_colindex(HoloMutation mutation, int colIndex, int32_t value)<br>int holo_client_set_req_int32_val_by_colname(HoloMutation mutation, const char* colName, int32_t value)<br>int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len)<br>int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) | |<br>
| bigint | int holo_client_set_req_int64_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value)<br>int holo_client_set_req_int64_val_by_colname(HoloMutation mutation, const char* colName, int64_t value)<br>int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len)<br>int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) | |<br>
| boolean | int holo_client_set_req_bool_val_by_colindex(HoloMutation mutation, int colIndex, bool value)<br>int holo_client_set_req_bool_val_by_colname(HoloMutation mutation, const char* colName, bool value)<br>int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len)<br>int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) | |<br>
| real/float4 | int holo_client_set_req_float_val_by_colindex(HoloMutation mutation, int colIndex, float value)<br>int holo_client_set_req_float_val_by_colname(HoloMutation mutation, const char* colName, float value)<br>int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len)<br>int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) | |<br>
| float/float8/double precision | int holo_client_set_req_double_val_by_colindex(HoloMutation mutation, int colIndex, double value)<br>int holo_client_set_req_double_val_by_colname(HoloMutation mutation, const char* colName, double value)<br>int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len)<br>int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) | |<br>
| text | int holo_client_set_req_text_val_by_colindex(HoloMutation mutation, int colIndex, const char* value)<br>int holo_client_set_req_text_val_by_colname(HoloMutation mutation, const char* colName, char* value)<br>int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len)<br>int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) | |<br>
| timestamp | int holo_client_set_req_timestamp_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value)<br>int holo_client_set_req_timestamp_val_by_colname(HoloMutation mutation, const char* colName, int64_t value) | 不带time zone的， 以int64的形式插入(值代表从2000-01-01 00:00:00起的微秒数) |<br>
| timestamptz | int holo_client_set_req_timestamptz_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value)<br>int holo_client_set_req_timestamptz_val_by_colname(HoloMutation mutation, const char* colName, int64_t value) | 带time zone的， 以int64的形式插入(值代表从2000-01-01 00:00:00起的微秒数) |<br>
| int[] | int holo_client_set_req_int32_array_val_by_colindex(HoloMutation mutation, int colIndex, int32_t* values, int nValues)<br>int holo_client_set_req_int32_array_val_by_colname(HoloMutation mutation, const char* colName, int32_t* values, int nValues) | |<br>
| bigint[] | int holo_client_set_req_int64_array_val_by_colindex(HoloMutation mutation, int colIndex, int64_t* values, int nValues)<br>int holo_client_set_req_int64_array_val_by_colname(HoloMutation mutation, const char* colName, int64_t* values, int nValues) | |<br>
| boolean[] | int holo_client_set_req_bool_array_val_by_colindex(HoloMutation mutation, int colIndex, bool* values, int nValues)<br>int holo_client_set_req_bool_array_val_by_colname(HoloMutation mutation, const char* colName, bool* values, int nValues) | |<br>
| real[] /float4[] | int holo_client_set_req_float_array_val_by_colindex(HoloMutation mutation, int colIndex, float* values, int nValues)<br>int holo_client_set_req_float_array_val_by_colname(HoloMutation mutation, const char* colName, float* values, int nValues) | |<br>
| float[] /float8[] | int holo_client_set_req_double_array_val_by_colindex(HoloMutation mutation, int colIndex, double* values, int nValues)<br>int holo_client_set_req_double_array_val_by_colname(HoloMutation mutation, const char* colName, double* values, int nValues) | |<br>
| text[] | int holo_client_set_req_text_array_val_by_colindex(HoloMutation mutation, int colIndex, char** values, int nValues)<br>int holo_client_set_req_text_array_val_by_colname(HoloMutation mutation, const char* colName, char** values, int nValues) | |<br>
| 其他类型 | int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, char* value, int len)<br>int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) | 所有类型都可以以text的形式插入 |<br>

### go 使用 holo-client
推荐使用 holo-client-c 的go包装 holo-client-go

也可以使用 cgo 调用 holo-client C 的动态库

基本类型(int16 int32 int64 bool float(float32) double(float64))可直接使用

基本数组类型(int[] int64[] bool[] float32[] float64[])可直接使用，切片不行

字符串数组(外层数组，内层C.CString)可以

timestamp, timestamptz以int64形式写入

string 需要转换成C.CString (需要手动C.free释放)

写入example:
```go
package main
/*
#cgo CFLAGS: -I./include
#cgo LDFLAGS: -L./lib -lholo-client
#include "holo_client.h"
#include "holo_config.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"

import "unsafe"

func main() {
	C.holo_client_logger_open()
	connInfo := "host=xxxx port=xx dbname=xxx user=xxx password=xxxxx"
	cInfo := C.CString(connInfo)
	config := C.holo_client_new_config(cInfo)
	config.writeMode = C.INSERT_OR_REPLACE;
	cTableName := C.CString("table_name")
	client := C.holo_client_new_client(config);
	schema := C.holo_client_get_tableschema(client, nil, cTableName, false)

	mutation := C.holo_client_new_mutation_request(schema);
	C.holo_client_set_req_int32_val_by_colindex(mutation, 0, 123)
	C.holo_client_set_req_bool_val_by_colindex(mutation, 1, true)
	content := C.CString("text")
	C.holo_client_set_req_text_val_by_colindex(mutation, 2, content, 4)
	C.holo_client_set_req_float_val_by_colindex(mutation, 3, 10.211)
	C.holo_client_set_req_double_val_by_colindex(mutation, 4, 10.211)
	intArray := [5] int32{1,2,3,4,5}
	C.holo_client_set_req_int32_array_val_by_colindex(mutation, 1, (*C.int)(unsafe.Pointer(&intArray)) , 5)
	decimal := C.CString("10.211")
	C.holo_client_set_req_val_with_text_by_colindex(mutation, 3, decimal, 6)
	C.holo_client_submit(client, mutation)
	C.free(unsafe.Pointer(content))
	C.free(unsafe.Pointer(decimal))
	
	C.holo_client_flush_client(client)
	C.holo_client_close_client(client)
	C.holo_client_logger_close()
	C.free(unsafe.Pointer(cInfo))
	C.free(unsafe.Pointer(cTableName))
}
```

点查example:
```go
package main

/*
#cgo CFLAGS: -I./include
#cgo LDFLAGS: -L./lib -lholo-client
#include "holo_client.h"
#include "holo_config.h"
#include "request.h"
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
	"fmt"
	"strconv"
)

func main() {
	C.holo_client_logger_open()
	connInfo := "host=xxxxx port=xxx dbname=xxxxx user=xxxxx password=xxxxx"
	cInfo := C.CString(connInfo)
	config := C.holo_client_new_config(cInfo)
	cSchemaName := C.CString("schema_name")
	cTableName := C.CString("table_name")
	client := C.holo_client_new_client(config);
	schema := C.holo_client_get_tableschema(client, cSchemaName, cTableName, false)

	for i := 0; i < 100; i++ {
		get := C.holo_client_new_get_request(schema)
		val := C.CString(strconv.Itoa(i)) //以string的方式设置pk
		C.holo_client_set_get_val_with_text_by_colindex(get, 0, val, len(val))
		C.holo_client_get(client, get);
	    res := C.holo_client_get_record(get)
		fmt.Printf("Record %d:\n", i)
		if res == nil { //查不到记录或者发生异常
			fmt.Println("No record")
		} else {
			for col := 0; col < int(schema.nColumns); col++ {
				col_val := C.GoString(C.holo_client_get_record_val(res,C.int(col))) //第col列的结果， string的形式
				fmt.Println(col_val) 
			}
		}
	    C.holo_client_destroy_get_request(get);
		C.free(unsafe.Pointer(val))
	}
	C.holo_client_flush_client(client)
	C.holo_client_close_client(client)
	C.holo_client_logger_close()
	C.free(unsafe.Pointer(cInfo))
	C.free(unsafe.Pointer(cTableName))
	C.free(unsafe.Pointer(cSchemaName))
}
```