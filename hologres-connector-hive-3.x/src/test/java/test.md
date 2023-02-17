数据类型测试

```sql
create table hive_customer_type(a int, b int8,c bool, d real, e float8, f varchar(5), g json, h jsonb,i timestamptz,j date, k numeric(18,5), l bytea, m int[], n bigint[], o real[], p double precision[], q bool[], r text[]);
 
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
    k  decimal(18,5),
    l  binary,
    m  ARRAY<int>,
    n  ARRAY<bigint>,
    o  ARRAY<float>,
    p  ARRAY<double>,
    q  ARRAY<boolean>,
    r  ARRAY<string>
)
STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_customer_type",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.copy_write_mode" = "true",
    "hive.sql.copy_write_max_connections_number" = "20",
    "hive.sql.copy_write_dirty_data_check" = "true"
);

insert into customer_to_holo_type select 111, 222, 'false', 1.23, 2.34,'ccc','{\"a\":\"b\"}','{\"a\":\"b\"}', '2021-05-21 16:00:45.123', '2021-05-21','85.23', '\x030405', array(1,2,3), array(1L,2L,3L), null, null, array(true,flase), array('a','b','c');
```