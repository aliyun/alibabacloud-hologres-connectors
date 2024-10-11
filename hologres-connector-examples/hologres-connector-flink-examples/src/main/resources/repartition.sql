--sourceDDL
CREATE TEMPORARY TABLE source_table
(
    c_custkey     BIGINT
    ,c_name       STRING
    ,c_address    STRING
    ,c_nationkey  INTEGER
    ,c_phone      STRING
    ,c_acctbal    NUMERIC(15, 2)
    ,c_mktsegment STRING
    ,c_comment    STRING
)
WITH (
    'connector' = 'datagen'
    ,'rows-per-second' = '10'
    ,'number-of-rows' = '100'
);

--sourceDql
select *, cast('2024-04-21' as DATE) from source_table;

--sinkDDL
CREATE TABLE sink_table
(
    c_custkey     BIGINT
    ,c_name       STRING
    ,c_address    STRING
    ,c_nationkey  INTEGER
    ,c_phone      STRING
    ,c_acctbal    NUMERIC(15, 2)
    ,c_mktsegment STRING
    ,c_comment    STRING
    ,`date`       DATE
)
with (
    'connector' = 'hologres'
    ,'dbname' = 'test_db'
    ,'tablename' = 'test_sink_customer'
    ,'username' = ''
    ,'password' = ''
    ,'endpoint' = ''
    ,'jdbccopywritemode' = 'BULK_LOAD'
    ,'reshuffle-by-holo-distribution-key.enabled'='true'
);
