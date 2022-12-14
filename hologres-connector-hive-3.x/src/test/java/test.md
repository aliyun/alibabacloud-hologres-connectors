数据类型测试

```sql
create table hive_customer_type(a int, b int8,c bool, d real, e float8, f text, g json, h jsonb,i timestamptz,j date,k numeric(18,5));

 
CREATE EXTERNAL TABLE customer_to_holo_type
(
    a  int,
    b  bigint,
    c  boolean,
    d  float,
    e  double,
    f  string,
    g  string,
    h  string,
    i  timestamp,
    j  date,
    k  decimal(18,5)
)
STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_customer_type",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.write_thread_size" = "12"
);

insert into customer_to_holo_type values (111, 222, 'false', 1.23, 2.34,'ccc','{\"a\":\"b\"}','{\"a\":\"b\"}','2021-05-21 16:00:45','2021-05-21','85.23');
```