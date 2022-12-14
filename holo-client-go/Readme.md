# 依赖holo-client-c golang 调用holo-client-c sdk

## 简介
golang 调用 holo-client-c 对 hologres 实例进行查询和写入的示例

其中holoclient package(./holo-client文件夹) 包装了所有的cgo调用  
调用holoclient package的示例在example.go中  
holo-client-c的函数具体用法见[holo-client-c](../holo-client-c)的 Readme和头文件  

### 功能介绍
holo-client适用于大批量数据写入。holo-client基于libpq实现，使用时请确认实例剩余可用连接数。

-查看最大连接数
```sql
show max_connections;
```
- 查看已使用连接数
```sql
select count(*) from pg_stat_activity;
```

### 需要安装的依赖库
holo-client-c依赖 libpq log4c jemalloc  
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
如果程序找不到动态库[libholo-client.so](./holo-client/lib) 可以尝试设置LD_LIBRARY_PATH  
如果动态库不可用，可以参考holo-client-c从源码生成动态库（推荐）


## 数据写入
建议项目中创建HoloClient单例，通过threadSize控制并发（每并发占用1个连接，空闲超过connectionMaxIdleMs将被自动回收)

### 写入普通表/含主键表
```go
holoclient.HoloClientLoggerOpen()
connInfo := "host=xxxx port=xx dbname=xxxxx user=xxxxx password=xxxxx"
config := holoclient.NewHoloConfig(connInfo)
config.SetWriteMode(holoclient.INSERT_OR_REPLACE)
client := holoclient.NewHoloClient(config)
//create table test_go_put (id int, f1 real, f2 boolean, f3 text, f4 int[], f5 boolean[], f6 real[], f7 float[], primary key(id))
schema := client.GetTableschema("", "test_go_put", false)

intArray := [] int32{3,4,5,6}
boolArray := [] bool{true, false, true}
floatArray := [] float32{1.123, 2.234}
doubleArray := [] float64{1.2345, 3.4567}
for i := 0; i < 100; i++ {
	put := holoclient.NewMutationRequest(schema)
	put.SetInt32ValByColIndex(0, int32(i)) //含主键表的主键必须要设置
	put.SetFloat32ValByColIndex(1, 123.234)
	put.SetBoolValByColIndex(2, true)
	put.SetTextValByColIndex(3, "hello")
	put.SetInt32ArrayValByColIndex(4, intArray)
	put.SetBoolArrayValByColIndex(5, boolArray)
	put.SetFloat32ArrayValByColIndex(6, floatArray)
	put.SetFloat64ArrayValByColIndex(7, doubleArray)
	client.Submit(put)
}

client.Flush()
client.Close()
holoclient.HoloClientLoggerClose()
```

### 写入分区表
若分区已存在，不论dynamicPartition为何值，写入数据都将插入到正确的分区表中；若分区不存在，dynamicPartition设置为true时，将会自动创建不存在的分区，否则插入失败
```go
holoclient.HoloClientLoggerOpen()
connInfo := "host=xxxx port=xx dbname=xxxxx user=xxxxx password=xxxxx"
config := holoclient.NewHoloConfig(connInfo)
config.SetWriteMode(holoclient.INSERT_OR_REPLACE)
config.SetDynamicPartition(true)
client := holoclient.NewHoloClient(config)

//create table schema_name.table_name (id int, name text, address text) partition by list(name)
schema := client.GetTableschema("schema_name", "table_name", true)

put := holoclient.NewMutationRequest(schema)
put.SetInt32ValByColIndex(0, 0)
put.SetTextValByColIndex(1, "name0")
put.SetTextValByColIndex(2, "address0")
client.Submit(put)

put = holoclient.NewMutationRequest(schema)
put.SetInt32ValByColIndex(0, 1)
put.SetTextValByColIndex(1, "name1")
put.SetTextValByColIndex(2, "address1")
client.Submit(put)

client.Flush()
client.Close()
holoclient.HoloClientLoggerClose()
```

### 根据主键删除
```go
holoclient.HoloClientLoggerOpen()
connInfo := "host=xxxx port=xx dbname=xxxxx user=xxxxx password=xxxxx"
config := holoclient.NewHoloConfig(connInfo)
client := holoclient.NewHoloClient(config)

//create table schema_name.table_name (id int, name text, address text, primary key(id))
schema := client.GetTableschema("schema_name", "table_name", true)

put := holoclient.NewMutationRequest(schema)
put.SetRequestMode(holoclient.DELETE)
put.SetInt32ValByColIndex(0, 0)
client.Submit(put) //删除必须设置所有的主键

put = holoclient.NewMutationRequest(schema)
put.SetRequestMode(holoclient.DELETE)
put.SetInt32ValByColIndex(0, 1)
client.Submit(put)

client.Flush()
client.Close()
holoclient.HoloClientLoggerClose()
```

### 写入失败的处理
如果有record写入失败会调用一个回调函数，这个回调函数在HoloConfig中设置（每条插入失败的record都会调用这个函数） 
C不能直接调用go的函数指针，C调用go的回调函数方法见https://github.com/golang/go/wiki/cgo#function-variables

### 根据主键点查
```go
holoclient.HoloClientLoggerOpen()
connInfo := "host=xxxxx port=xxx dbname=xxxx user=xxxxx password=xxxxx"
config := holoclient.NewHoloConfig(connInfo)
client := holoclient.NewHoloClient(config)
//create table schema_name.table_name (id int, name text, address text, primary key(id))
schema := client.GetTableschema("schema_name", "table_name", true)

for i := 0; i < 100; i++ {
	get := holoclient.NewGetRequest(schema)
	get.SetGetValByColIndex(0, strconv.Itoa(i)) //点查必须设置所有的主键
	res := client.Get(get) //可以多线程调用client点查提高效率
	fmt.Printf("Record %d:\n", i)
	if res == nil { //查不到记录或者发生异常
		fmt.Println("No record")
	} else {
		for col := 0; col < schema.NumColumns(); col++ {
			col_val := res.GetVal(col) //第col列的结果， string的形式
			fmt.Println(col_val)
		}
	}
	get.DestroyGet() //得到结果后需要手动释放
}

client.Flush()
client.Close()
holoclient.HoloClientLoggerClose()
```

## 附录
### HoloConfig参数说明
| 参数名 | 默认值 | 说明 |
| --- | --- | --- |
| connInfo| 无 | 必填, 格式:“host=xxxxxxxxx port=xxx dbname=xxxxx user=xxxxxxxxxx password=xxxxxxxxxx”| 
| dynamicPartition | false | 若为true，当分区不存在时自动创建分区 |
| writeMode | INSERT_OR_REPLACE | 当INSERT目标表为有主键的表时采用不同策略<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE当主键冲突时，更新所有列|
| batchSize | 512 | 每个写入线程的最大批次大小，在经过WriteMode合并后的插入数量达到batchSize时进行一次批量提交 |
| threadSize | 3 | 写入并发线程数（每个并发占用1个数据库连接） |
| shardCollectorSize | 6 | 每个表的缓冲区个数，建议shardCollectorSize大于threadSize |
| writeBatchByteSize | 2MB | 每个写入线程的最大批次bytes大小，在经过WriteMode合并后的插入数据字节数达到writeBatchByteSize时进行一次批量提交 |
| writeBatchTotalByteSize | writeBatchByteSize * shardCollectorSize | 所有表所有缓冲区所有数据总和最大bytes大小，达到writeBatchTotalByteSize时全部提交 |
| writeMaxIntervalMs | 10000 ms | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交 |
| exceptionHandler | handle_exception_by_doing_nothing | 对于写入失败的数据的处理函数 |
| retryCount | 3 | 当连接故障时的重试次数 |
| retrySleepInitMs | 1000 ms | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |
| retrySleepStepMs | 10*1000 ms | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |
| connectionMaxIdleMs| 60000 ms | 写入线程数据库连接的最大Idle时间，超过连接将被释放|


### LOG说明
holo client用的是log库是log4c

在所有跟holo client有关的代码开始前需要调用 holo_client_logger_open();
结束后需要调用 holo_client_logger_close();

注意

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
具体函数定义见[request.go](./holo-client/request.go) 和[request.h](./holo-client/include/request.h) 和[holo-clkient-c](../holo-client-c)的Readme


### go 直接调用holo-client-c 示例
使用cgo 调用 holo-client C 的动态库

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
	C.holo_client_set_req_text_val_by_colindex(mutation, 2, content)
	C.holo_client_set_req_float_val_by_colindex(mutation, 3, 10.211)
	C.holo_client_set_req_double_val_by_colindex(mutation, 4, 10.211)
	intArray := [5] int32{1,2,3,4,5}
	C.holo_client_set_req_int32_array_val_by_colindex(mutation, 1, (*C.int)(unsafe.Pointer(&intArray)) , 5)
	decimal := C.CString("10.211")
	C.holo_client_set_req_val_with_text_by_colindex(mutation, 3, decimal)
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
		C.holo_client_set_get_val_with_text_by_colindex(get, 0, val)
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