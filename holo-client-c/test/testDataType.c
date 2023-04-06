#include "holo_client.h"
#include "table_schema.h"
#include "holo_config.h"
#include "holo_client_private.h"
#include <libpq-fe.h>
#include <CUnit/Basic.h>
#include <stdint.h>

char* connInfo;

/*
 * float4, float8, decimal.
 */
void testDecimal() {
    // printf("---------test float float8 decimal--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_decimal";
    char* dropSql = "drop table if exists holo_client_put_decimal";
    char* createSql = "create table holo_client_put_decimal (id int not null, f1 float4, f2 float8, f3 decimal(12,2),primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_float_val_by_colindex(mutation, 1, 10.211);
    holo_client_set_req_double_val_by_colindex(mutation, 2, 10.211);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "10.211", 6);
    holo_client_submit(client, mutation);

    //以text的形式插入
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "1", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "11.211", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "11.211", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "11.211", 6);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "2", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "11.211", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "11.211", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "11.211", 6);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "3", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "11.211", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "11.211", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "11.211", 6);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_decimal where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "10.211");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "10.211");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "10.21");
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_decimal where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "11.211");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "11.211");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "11.21");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/*
 * smallint, int, bigint, bool, text.
 */
void testBasicTypes() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_basic";
    char* dropSql = "drop table if exists holo_client_put_basic";
    char* createSql = "create table holo_client_put_basic (id int not null, f1 smallint, f2 bigint, f3 boolean, f4 text ,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 123);
    holo_client_set_req_int16_val_by_colindex(mutation, 1, 123);
    holo_client_set_req_int64_val_by_colindex(mutation, 2, 123);
    holo_client_set_req_bool_val_by_colindex(mutation, 3, true);
    holo_client_set_req_text_val_by_colindex(mutation, 4, "\"text", 5);
    holo_client_submit(client, mutation);

    //以text的形式插入
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "456", 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "456", 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "456", 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "false", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "\\text", 5);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_basic where id = 123");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "123");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "123");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "t");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 4), "\"text");
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_basic where id = 456");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "456");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "456");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "f");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 4), "\\text");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/*
 * int[], bigint[], bool[], float4[], float8[], text[].
 */
void testArrayTypes() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_array";
    char* dropSql = "drop table if exists holo_client_put_array";
    char* createSql = "create table holo_client_put_array (id int not null, f1 int[], f2 bigint[], f3 boolean[], f4 float4[], f5 float8[], f6 text[] ,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    char* textArray[] = {"hello", "world"};
    int intArray[] = {1, 2, 3};
    int64_t bigintArray[] = {11111111, 222222222, 333333333};
    bool boolArray[] = {true, true, false};
    float floatArray[] = {1.23, 4.56};
    double doubleArray[] = {1.234, 5.678};

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_array_val_by_colindex(mutation, 1, intArray, 3);
    holo_client_set_req_int64_array_val_by_colindex(mutation, 2, bigintArray, 3);
    holo_client_set_req_bool_array_val_by_colindex(mutation, 3, boolArray, 3);
    holo_client_set_req_float_array_val_by_colindex(mutation, 4, floatArray, 2);
    holo_client_set_req_double_array_val_by_colindex(mutation, 5, doubleArray, 2);
    holo_client_set_req_text_array_val_by_colindex(mutation, 6, textArray, 2);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_array where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "{1,2,3}");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "{11111111,222222222,333333333}");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "{t,t,f}");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 4), "{1.23,4.56}");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 5), "{1.234,5.678}");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 6), "{hello,world}");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/*
 * bytea, timestamp, timestamptz, varchar.
 */
void testOtherTypes() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_other";
    char* dropSql = "drop table if exists holo_client_put_other";
    char* createSql = "create table holo_client_put_other (id int not null, f1 bytea, f2 timestamp, f3 timestamptz, f4 varchar(10) ,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);

    //以text的形式插入
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "0", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "\\x313233", 8);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "2021-04-12 12:12:12", 19);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "2021-04-12 12:12:12", 19);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "text", 4);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "1", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "\\x313233", 8);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "2021-04-12 12:12:12", 19);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "2021-04-12 12:12:12", 19);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "text", 4);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "2", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "\\x313233", 8);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "2021-04-12 12:12:12", 19);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "2021-04-12 12:12:12", 19);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "text", 4);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "3", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "\\x313233", 8);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "2021-04-12 12:12:12", 19);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "2021-04-12 12:12:12", 19);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "text", 4);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_other where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "\\x313233");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "2021-04-12 12:12:12");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "2021-04-12 12:12:12+08");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 4), "text");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/*
 * json, jsonb, date, char.
 */
void testOtherTypes2(void) {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_other2";
    char* dropSql = "drop table if exists holo_client_put_other2";
    char* createSql = "create table holo_client_put_other2 (c0 int, c1 json, c2 jsonb, c3 date, c4 char, primary key(c0))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "{\"a\":1,\"b\":2}", 13);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "{\"a\":1,\"b\":2}", 13);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "2022-09-20", 10);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "a", 1);
    // char charArray[] = {'a','b','c'};
    // holo_client_set_req_text_array_val_by_colindex(mutation, 5, charArray, 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "{\"a\":1,\"b\":2}", 13);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "{\"a\":1,\"b\":2}", 13);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "2022-09-20", 10);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "a", 1);
    // char charArray[] = {'a','b','c'};
    // holo_client_set_req_text_array_val_by_colindex(mutation, 5, charArray, 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "{\"a\":1,\"b\":2}", 13);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "{\"a\":1,\"b\":2}", 13);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "2022-09-20", 10);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "a", 1);
    // char charArray[] = {'a','b','c'};
    // holo_client_set_req_text_array_val_by_colindex(mutation, 5, charArray, 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "{\"a\":1,\"b\":2}", 13);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "{\"a\":1,\"b\":2}", 13);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "2022-09-20", 10);
    holo_client_set_req_val_with_text_by_colindex(mutation, 4, "a", 1);
    // char charArray[] = {'a','b','c'};
    // holo_client_set_req_text_array_val_by_colindex(mutation, 5, charArray, 3);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_other2 where c0 = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "{\"a\":1,\"b\":2}");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "{\"a\": 1, \"b\": 2}");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "2022-09-20");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 4), "a");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/*
 * timestamp timestamptz.
 */
void testTimestamp() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_timestamp";
    char* dropSql = "drop table if exists holo_client_put_timestamp";
    char* createSql = "create table holo_client_put_timestamp (id int not null, f1 timestamp, f2 timestamptz, primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_timestamp_val_by_colindex(mutation, 1, 0);
    holo_client_set_req_timestamptz_val_by_colindex(mutation, 2, 0);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_timestamp_val_by_colindex(mutation, 1, 86400000000);
    holo_client_set_req_timestamptz_val_by_colindex(mutation, 2, 86400000000);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_timestamp where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "2000-01-01 00:00:00");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "2000-01-01 08:00:00+08");
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_timestamp where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "2000-01-02 00:00:00");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "2000-01-02 08:00:00+08");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

// int main(int argc, char** argv) {
//   holo_client_logger_open();
//   testDecimal();
//   testBasicTypes();
//   testArrayTypes();
//   testOtherTypes();
//   testOtherTypes2();
//   testTimestamp();
//   holo_client_logger_close();
//   return 0;
// }