#include "holo_client.h"
#include "table_schema.h"
#include "holo_config.h"
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <CUnit/Basic.h>

char* connInfo;

void* print_failed_record(const HoloRecord* record, const char* errMsg, void* exceptionHandlerParam) {
    printf("---------------------failed record:\n");
    printf("error mesesage: %s\n", errMsg);
    for (int i = 0; i < holo_client_record_num_column(record); i++) {
        char* val = holo_client_get_record_val_with_text_by_colindex(record, i);
        printf("value %d: %s ", i, val);
        holo_client_destroy_val(val);
    }
    printf("\n");
    return NULL;
}

void testConnectFail() {
    HoloConfig config = holo_client_new_config("host=test_host port=8080 dbname=test_db user=test_user password=test_password");
    config.retrySleepStepMs = 1000;
    HoloClient* client = holo_client_new_client(config);
    char* errMsg = NULL;
    HoloTableSchema* schema = holo_client_get_tableschema_with_errmsg(client, NULL, "test_table", true, &errMsg);

    CU_ASSERT_EQUAL(schema, NULL);
    CU_ASSERT_NOT_EQUAL(errMsg, NULL);
    // should failed by "Name or service not known"
    printf("get table schema failed: %s", errMsg);
    if (errMsg != NULL) {
        free(errMsg);
    }

    holo_client_close_client(client);
}

void testGet() {
    HoloConfig config = holo_client_new_config(connInfo);
    PGconn * conn = PQconnectdb(connInfo);

    char* tableName = "holo_client_get";
    char* dropSql = "drop table if exists holo_client_get";
    char* createSql = "create table holo_client_get (id int not null,name text,address text,primary key(id))";
    char* insertSql = "insert into holo_client_get values(0, 'name0', 'address')";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    PQclear(PQexec(conn, insertSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloGet get = holo_client_new_get_request(schema);
    holo_client_set_get_val_with_text_by_colindex(get, 0, "0", 1);
    holo_client_get(client, get);

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

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * INSERT_REPLACE.
 */
void testPut001() {
    //printf("---------testPut001--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_001";
    char* dropSql = "drop table if exists holo_client_put_001";
    char* createSql = "create table holo_client_put_001 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name3", 5);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_001 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name3");
    CU_ASSERT(PQgetisnull(res, 0, 2));
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_001 where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT(PQgetisnull(res, 0, 2));
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * INSERT_IGNORE.
 */
void testPut002() {
    // printf("---------testPut002--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_002";
    char* dropSql = "drop table if exists holo_client_put_002";
    char* createSql = "create table holo_client_put_002 (id int not null,name text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name3", 5);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_002 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_002 where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_002 where id = 2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT(PQgetisnull(res, 0, 1));
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * INSERT_UPDATE.
 */
void testPut003() {
    // printf("---------testPut003--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_003";
    char* dropSql = "drop table if exists holo_client_put_003";
    char* createSql = "create table holo_client_put_003 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2", 8);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address3", 8);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_003 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    CU_ASSERT(PQgetisnull(res, 0, 2));
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_003 where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address3");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * CUSTOM_SCHEMA，INSERT_OR_UPDATE.
 */
void testPut004() {
    // printf("---------testPut004--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_004";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_004";
    char* createSql = "create table test_schema.holo_client_put_004 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2", 8);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address3", 8);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_004 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    CU_ASSERT(PQgetisnull(res, 0, 2));
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_004 where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address3");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * CUSTOM_SCHEMA，success.
 */
void testPut005() {
    // printf("---------testPut005--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_005";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_005";
    char* createSql = "create table test_schema.holo_client_put_005 (id int not null,name text,address text,primary key(id,name)) partition by list(name)";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_005 where id = 0 and name = 'name0'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * CUSTOM_SCHEMA，fail.
 */
void testPut006() {
    // printf("---------testPut006--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.exceptionHandler = print_failed_record;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_006";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_006";
    char* createSql = "create table test_schema.holo_client_put_006 (id int not null,name text,address text,primary key(id,name)) partition by list(name)";
    char* createSql1 = "create table test_schema.holo_client_put_006 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    printf("-----------should have two failed put------------\n");

    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql1));

    schema = holo_client_get_tableschema(client, "test_schema", tableName, false);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_006 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * CUSTOM_SCHEMA, tableName, columnName in PG RESERVED KEYWORDS
 */
void testPut007() {
    // printf("---------testPut007--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holO_client\"_put_007";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.\"holO_client\"\"_put_007\"";
    char* createSql = "create table test_schema.\"holO_client\"\"_put_007\" (id int not null,\"nAme\" text,address text,\"user\" text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "user1", 5);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2", 8);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "user2", 5);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.\"holO_client\"\"_put_007\" where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "user1");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.\"holO_client\"\"_put_007\" where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address2");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "user2");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * put null
 */
void testPut008() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.exceptionHandler = print_failed_record;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_008";
    char* dropSql = "drop table if exists holo_client_put_008";
    char* createSql = "create table holo_client_put_008 (id text, int4_field int, int8_field bigint, numeric_field numeric(38,15), text_field text, primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_text_val_by_colindex(mutation, 0, "1", 1);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_int64_val_by_colindex(mutation, 2, 1);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_text_val_by_colindex(mutation, 0, "2", 1);
    holo_client_set_req_null_val_by_colindex(mutation, 1);
    holo_client_set_req_null_val_by_colindex(mutation, 2);
    holo_client_set_req_null_val_by_colindex(mutation, 3);
    holo_client_set_req_text_val_by_colindex(mutation, 4, "text", 4);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_text_val_by_colindex(mutation, 0, "3", 1);
    holo_client_set_req_null_val_by_colindex(mutation, 1);
    holo_client_set_req_null_val_by_colindex(mutation, 2);
    holo_client_set_req_text_val_by_colindex(mutation, 4, "text", 4);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_text_val_by_colindex(mutation, 0, "4", 1);
    holo_client_set_req_text_val_by_colindex(mutation, 4, "text", 4);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_008 where id = '1'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "1");
    CU_ASSERT(PQgetisnull(res, 0, 3));
    CU_ASSERT(PQgetisnull(res, 0, 4));
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_008 where id = '2'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT(PQgetisnull(res, 0, 1));
    CU_ASSERT(PQgetisnull(res, 0, 2));
    CU_ASSERT(PQgetisnull(res, 0, 3));
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 4), "text");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * put data overflow, failed
 */
void testPut009() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.exceptionHandler = print_failed_record;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_009";
    char* dropSql = "drop table if exists holo_client_put_009";
    char* createSql = "create table holo_client_put_009 (id int, name varchar(5), numeric_field numeric(3,0), primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "100", 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name11", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "100", 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name22", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "100.01", 6);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name33", 6);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "100.011", 7);
    holo_client_submit(client, mutation);

    char* errMsg = NULL;
    int ret = holo_client_flush_client_with_errmsg(client, &errMsg);
    // should failed by "value too long for type character varying(5)"
    printf("holo client flush failed: %s\n", errMsg);
    CU_ASSERT_NOT_EQUAL(errMsg, NULL);
    if (errMsg != NULL) {
        free(errMsg);
    }

    CU_ASSERT_EQUAL(ret, HOLO_CLIENT_FLUSH_FAIL);

    res = PQexec(conn, "select * from holo_client_put_009 where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "100");
    PQclear(res);

    res = PQexec(conn, "select * from holo_client_put_009 where id = 2");
    CU_ASSERT_EQUAL(PQntuples(res), 0);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * column name with char '"'
 */
void testPut010() {
    // printf("---------testPut010--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holO_client_put_010";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.\"holO_client_put_010\"";
    char* createSql = "create table test_schema.\"holO_client_put_010\" (id int not null,\"a\"\"b\" text,\"address\" text not null,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    CU_ASSERT_STRING_EQUAL(schema->columns[1].name, "a\"b");

    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.\"holO_client_put_010\" where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address2");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * test WriteFailStrategy.TRY_ONE_BY_ONE
 */
void testPut011() {
    // printf("---------testPut011--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    config.exceptionHandler = print_failed_record;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holO_client_put_011";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.\"holO_client_put_011\"";
    char* createSql = "create table test_schema.\"holO_client_put_011\" (id int not null default 0,\"a\"\"b\" text,\"address\" text not null,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    CU_ASSERT_STRING_EQUAL(schema->columns[1].name, "a\"b");

    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name2", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, NULL, 0);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name3", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address3", 8);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);
    printf("-----------should have one failed insert-----------------\n");

    res = PQexec(conn, "select * from test_schema.\"holO_client_put_011\" where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.\"holO_client_put_011\" where id = 2");
    CU_ASSERT_EQUAL(PQntuples(res), 0);
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.\"holO_client_put_011\" where id = 3");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name3");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address3");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * Put by colName
 */
void testPut012() {
    // printf("---------testPut012--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holO_client_put_012";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.\"holO_client_put_012\"";
    char* createSql = "create table test_schema.\"holO_client_put_012\" (id int not null,\"a\"\"b\" text,\"address\" text not null,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    CU_ASSERT_STRING_EQUAL(schema->columns[1].name, "a\"b");

    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.\"holO_client_put_010\" where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address2");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * Put no pk
 */
void testPut013() {
    // printf("---------testPut013--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holO_client_put_013";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.\"holO_client_put_013\"";
    char* createSql = "create table test_schema.\"holO_client_put_013\" (id int not null,\"a\"\"b\" text,\"address\" text not null)";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    CU_ASSERT_STRING_EQUAL(schema->columns[1].name, "a\"b");

    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from test_schema.\"holO_client_put_013\"");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 4);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * connection max idle time
 */
void testPut014() {
    // printf("---------testPut014--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    config.connectionMaxIdleMs = 10000;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holO_client_put_014";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.\"holO_client_put_014\"";
    char* createSql = "create table test_schema.\"holO_client_put_014\" (id int not null,\"a\"\"b\" text,\"address\" text not null)";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    sleep(10);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    sleep(5);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    sleep(5);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);
    
    res = PQexec(conn, "select count(*) from test_schema.\"holO_client_put_014\"");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 7);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * put then delete
 */
void testPut015() {
    // printf("---------testPut015--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.dynamicPartition = true;
    config.connectionMaxIdleMs = 10000;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holO_client_put_015";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.\"holO_client_put_015\"";
    char* createSql = "create table test_schema.\"holO_client_put_015\" (id int not null,\"a\"\"b\" text,\"address\" text not null,primary key(id,\"a\"\"b\"))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 2);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name2", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 3);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name3", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 4);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2", 8);
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1", 5);
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "ccc", 3);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);
 
    res = PQexec(conn, "select count(*) from test_schema.\"holO_client_put_015\"");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 3);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete with batch
 */
void testPut016() {
    // printf("---------testPut016--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.shardCollectorSize = 1;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_016";
    char* dropSql = "drop table if exists holo_client_put_016";
    char* createSql = "create table holo_client_put_016 (id int, name text, address int, primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);

    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "name", "name1", 5);
    holo_client_set_req_int32_val_by_colname(mutation, "address", 1);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 2);
    holo_client_set_req_val_with_text_by_colname(mutation, "name", "name2", 5);
    holo_client_set_req_int32_val_by_colname(mutation, "address", 2);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);
    
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "name", "name1", 5);
    holo_client_set_req_int32_val_by_colname(mutation, "address", 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 2);
    holo_client_set_req_val_with_text_by_colname(mutation, "name", "name1", 5);
    holo_client_set_req_null_val_by_colname(mutation, "address");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);
 
    res = PQexec(conn, "select count(*) from holo_client_put_016");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 0);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * CUSTOM_SCHEMA，INSERT_OR_UPDATE
 */
void testPut017() {
    // printf("---------testPut017--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_017";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_017";
    char* createSql = "create table test_schema.holo_client_put_017 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
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

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0_new", 9);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0_new", 12);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_017 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0_new");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address0_new");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_017 where id = 2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_017 where id = 1");
    CU_ASSERT_EQUAL(PQntuples(res), 0);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * Put 数据1、数据2（写入失败），等待内部flush，Put 数据3，主动flush，应该能查到数据1和数据3.
 */
void testPut018() {
    // printf("---------testPut018--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.writeMaxIntervalMs = 3000;
    config.exceptionHandler = print_failed_record;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_018";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_018";
    char* createSql = "create table test_schema.holo_client_put_018 (id int not null,name text not null,address text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0", 8);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, NULL, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);

    //等待超过WriteMaxIntervalMs时间后自动提交
    sleep(5);

    printf("-----------should have one failed put------------\n");\

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name2", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_018 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address0");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_018 where id = 1");
    CU_ASSERT_EQUAL(PQntuples(res), 0);
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_018 where id = 2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name2");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address2");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * 关掉自动flush，攒批攒满之后submit应该失败
 */
void testPut019() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.batchSize = 5;
    config.autoFlush = false;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    int failedCount = 0;

    char* tableName = "holo_client_put_019";
    char* dropSql = "drop table if exists holo_client_put_019";
    char* createSql = "create table holo_client_put_019 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    for (int i = 0; i < 20; i++) {
        HoloMutation mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
        holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
        if (holo_client_submit(client, mutation) == HOLO_CLIENT_EXCEED_MAX_NUM) {
            ++failedCount;
        }
    }

    holo_client_flush_client(client);

    CU_ASSERT_EQUAL(failedCount, 10);
    res = PQexec(conn, "select count(*) from holo_client_put_019");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 0), "10");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * 关掉自动flush，长时间不调用flush
 */
void testPut020() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.batchSize = 10;
    config.writeMaxIntervalMs = 5000;
    config.autoFlush = false;
    config.shardCollectorSize = config.threadSize;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    int ret;

    char* tableName = "holo_client_put_020";
    char* dropSql = "drop table if exists holo_client_put_020";
    char* createSql = "create table holo_client_put_020 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
    ret = holo_client_submit(client, mutation);
    CU_ASSERT_EQUAL(ret, HOLO_CLIENT_RET_OK);

    sleep(3);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
    ret = holo_client_submit(client, mutation);
    CU_ASSERT_EQUAL(ret, HOLO_CLIENT_RET_OK);

    sleep(3); // 距上次flush超过writeMaxIntervalMs

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
    // should fail
    ret = holo_client_submit(client, mutation);
    CU_ASSERT_EQUAL(ret, HOLO_CLIENT_EXCEED_MAX_INTERVAL);

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_put_020");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 0), "2");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * 加列 加列需要先flush 然后重新拿table schema
 */
void testPut021() {
    // printf("---------testPut021--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_021";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_021";
    char* createSql = "create table test_schema.holo_client_put_021 (id int8 not null,name text not null,address text,primary key(id))";
    char* addColumnSql = "alter table test_schema.holo_client_put_021 add column new_c text";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int64_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0", 8);
    holo_client_submit(client, mutation);

    PQclear(PQexec(conn, addColumnSql));

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int64_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    schema = holo_client_get_tableschema(client, "test_schema", tableName, false);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int64_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name2", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2", 8);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "new", 3);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_021 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address0");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_021 where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_021 where id = 2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name2");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address2");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * execPreparedParams报错
 */
void testPut022() {
    // printf("---------testPut022--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.exceptionHandler = print_failed_record;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_022";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_022;drop table if exists test_schema.holo_client_put_022_1";
    char* createSql = "create table test_schema.holo_client_put_022 (id int not null,ts timestamp,primary key(id));create table test_schema.holo_client_put_022_1 (id int not null,ts timestamp,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloTableSchema* schema1 = holo_client_get_tableschema(client, "test_schema", "holo_client_put_022_1", true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2017-05-01 12:10:55", 19);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2017-05-01 12:10:55", 19);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2017-05-01 12:10:55", 19);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2017-05-01 12:10:55", 19);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema1);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "201705 12:10:55", 15);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema1);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "201705 12:10:55", 15);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema1);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "201705 12:10:55", 15);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema1);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "201705 12:10:55", 15);
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    printf("-----------should have one failed put------------\n");\

    res = PQexec(conn, "select count(*) from test_schema.holo_client_put_022");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 4);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * multiple primary key
 */
void testPut024() {
    // printf("---------testPut024--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_024";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_024";
    char* createSql = "create table test_schema.holo_client_put_024 (id int not null,name text not null,address text,primary key(id, name))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "a", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "b", 1);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "b", 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "c", 1);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_024 where id = 0 and name = 'a'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "b");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_024 where id = 0 and name = 'b'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "c");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * partition int
 */
void testPut025() {
    // printf("---------testPut025--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_025";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_025";
    char* createSql = "create table test_schema.holo_client_put_025 (id int not null,c int not null,d text,primary key(id,c)) partition by list(c)";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa", 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "123", 3);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_025 where id = 0 and c = 12");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "aaa");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_025 where id = 0 and c = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "123");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * partition text
 */
void testPut026() {
    // printf("---------testPut026--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_026";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_026";
    char* createSql = "create table test_schema.holo_client_put_026 (id int not null,c int not null,d text,primary key(id,d)) partition by list(d)";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa", 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "123", 3);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_026 where id = 0 and d = 'aaa'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "12");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_026 where id = 0 and d = '123'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "1");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * 局部更新
 */
void testPut027() {
    // printf("---------testPut027--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_027";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_027";
    char* createSql = "create table test_schema.holo_client_put_027 (id int not null,c int not null,d text,e text, primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa", 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "123", 3);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa", 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "123", 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa", 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "123", 3);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "456", 3);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_027 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "aaa");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "123");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_027 where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "aaa");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "123");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_027 where id = 2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "456");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 3), "123");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * put records to same partition.
 */
void testPut031() {
    // printf("---------testPut031--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_031";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_031";
    char* createSql = "create table test_schema.holo_client_put_031 (id int not null,name text,address text,primary key(id,name)) partition by list(name)";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2019-01-02", 10);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1", 8);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_031 where id = 0 and name = '2019-01-02'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2019-01-02", 10);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "abc", 3);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_031 where id = 0 and name = '2019-01-02'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "abc");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete then put,insert_or_update
 */
void testPut036() {
    // printf("---------testPut036--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_036";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_036";
    char* createSql = "create table test_schema.holo_client_put_036 (id int not null,name text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, -1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name-1", 6);
    holo_client_submit(client, mutation);
    char buffer[10];
    for (int i = 0; i < 500; i++) {
        sprintf(buffer, "name%d", i);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_request_mode(mutation, DELETE);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_submit(client, mutation);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer, strlen(buffer));
        holo_client_submit(client, mutation);
    }

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*),max(id),min(id) from test_schema.holo_client_put_036");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 501);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 1)), 499);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 2)), -1);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete then put,insert_or_ignore
 */
void testPut037() {
    // printf("---------testPut037--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_037";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_037";
    char* createSql = "create table test_schema.holo_client_put_037 (id int not null,name text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, -1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name-1", 6);
    holo_client_submit(client, mutation);
    char buffer[10];
    for (int i = 0; i < 500; i++) {
        sprintf(buffer, "name%d", i);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_request_mode(mutation, DELETE);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_submit(client, mutation);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer, strlen(buffer));
        holo_client_submit(client, mutation);
    }

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*),max(id),min(id) from test_schema.holo_client_put_037");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 501);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 1)), 499);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 2)), -1);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete then put,insert_or_replace
 */
void testPut038() {
    // printf("---------testPut038--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_038";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_038";
    char* createSql = "create table test_schema.holo_client_put_038 (id int not null,name text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, -1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name-1", 6);
    holo_client_submit(client, mutation);
    char buffer[10];
    for (int i = 0; i < 500; i++) {
        sprintf(buffer, "name%d", i);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_request_mode(mutation, DELETE);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_submit(client, mutation);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer, strlen(buffer));
        holo_client_submit(client, mutation);
    }

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*),max(id),min(id) from test_schema.holo_client_put_038");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 501);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 1)), 499);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 2)), -1);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete
 */
void testPut040() {
    // printf("---------testPut040--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_040";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_040";
    char* createSql = "create table test_schema.holo_client_put_040 (id int not null,id2 int not null, name text,primary key(id,id2))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    int count = 100000;
    HoloMutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_set_req_int32_val_by_colindex(mutation, 1, i+1);
        holo_client_set_req_val_with_text_by_colindex(mutation, 2, "name-1", 6);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    for (int i = 0; i < count - 2; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_request_mode(mutation, DELETE);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_set_req_int32_val_by_colindex(mutation, 1, i+1);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from test_schema.holo_client_put_040");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 2);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * put decimal
 */
void testPut042() {
    // printf("---------testPut042--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_042";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_042";
    char* createSql = "create table test_schema.holo_client_put_042 (id int not null,amount decimal(12,2),primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "16.211", 6);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_042 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "16.21");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.holo_client_put_042 where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT(PQgetisnull(res, 0, 1));
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * insert delete insert
 */
void testPut043() {
    // printf("---------testPut043--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_043";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_043";
    char* createSql = "create table test_schema.holo_client_put_043 (id int not null,amount decimal(12,2),primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "16.211", 6);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "19.211", 6);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_043 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "19.21");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * insert replace
 */
void testPut044() {
    // printf("---------testPut044--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_044";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_044";
    char* createSql = "create table test_schema.holo_client_put_044 (id int not null,amount decimal(12,2),primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    HoloMutation mutation;
    char buffer[10];
    for (int i = 0; i < 100; i++) {
        sprintf(buffer, "%d", i);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer, strlen(buffer));
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);


    res = PQexec(conn, "select * from test_schema.holo_client_put_044 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "99.00");
    PQclear(res);

    for (int i = 0; i < 100; i++) {
        sprintf(buffer, "%d", i);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer, strlen(buffer));
        holo_client_submit(client, mutation);
    }

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_044 where id = 1");
    CU_ASSERT_EQUAL(PQntuples(res), 0);
    PQclear(res);

    for (int i = 0; i < 100; i++) {
        sprintf(buffer, "%d", i);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer, strlen(buffer));
        if (i == 98) {
            holo_client_set_request_mode(mutation, DELETE);
        }
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_044 where id = 2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "99.00");
    PQclear(res);

    for (int i = 0; i < 100; i++) {
        sprintf(buffer, "%d", i);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer, strlen(buffer));
        if (i == 98) {
            holo_client_set_request_mode(mutation, DELETE);
        }
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_044 where id = 2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "99.00");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete in partition table
 */
void testPut047() {
    // printf("---------testPut047--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_047";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_047";
    char* createSql = "create table test_schema.holo_client_put_047 (id int not null,id2 int not null, name text,primary key(id,id2)) partition by list(id)";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    int count = 10000;
    HoloMutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i/1000);
        holo_client_set_req_int32_val_by_colindex(mutation, 1, i+1);
        holo_client_set_req_val_with_text_by_colindex(mutation, 2, "name-1", 6);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    for (int i = 0; i < count-2; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_request_mode(mutation, DELETE);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i/1000);
        holo_client_set_req_int32_val_by_colindex(mutation, 1, i+1);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from test_schema.holo_client_put_047");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 2);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete in partition table, partition key case sensitive
 */
void testPut048() {
    // printf("---------testPut048--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_048";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.holo_client_put_048";
    char* createSql = "create table test_schema.holo_client_put_048 (iD int not null,id2 int not null, name text,primary key(id,id2)) partition by list(iD)";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    int count = 10000;
    HoloMutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colname(mutation, "id", i/1000);
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_set_req_val_with_text_by_colname(mutation, "name", "name-1", 6);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    for (int i = 0; i < count-2; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_request_mode(mutation, DELETE);
        holo_client_set_req_int32_val_by_colname(mutation, "id", i/1000);
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from test_schema.holo_client_put_048");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 2);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete in partition table, public schema
 */
void testPut049() {
    // printf("---------testPut049--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_049";
    char* dropSql = "drop table if exists holo_client_put_049";
    char* createSql = "create table holo_client_put_049 (iD int not null,id2 int not null, name text,primary key(id,id2)) partition by list(iD)";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    int count = 10000;
    HoloMutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colname(mutation, "id", i/1000);
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_set_req_val_with_text_by_colname(mutation, "name", "name-1", 6);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    for (int i = 0; i < count-2; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_request_mode(mutation, DELETE);
        holo_client_set_req_int32_val_by_colname(mutation, "id", i/1000);
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_put_049");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 2);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete in partition table, public schema， int
 */
void testPut050() {
    // printf("---------testPut050--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_050";
    char* dropSql = "drop table if exists holo_client_put_050";
    char* createSql = "create table holo_client_put_050 (iD int not null,id2 int not null, name int,primary key(id,id2)) partition by list(iD)";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    int count = 100000;
    HoloMutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colname(mutation, "id", i/10000);
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_set_req_int32_val_by_colname(mutation, "name", i);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_put_050 where id2 = name + 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), count);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * delete in partition table, public schema, text
 */
void testPut051() {
    // printf("---------testPut051--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_UPDATE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_051";
    char* dropSql = "drop table if exists holo_client_put_051";
    char* createSql = "create table holo_client_put_051 (id text not null,id2 int not null, name int, primary key(id,id2)) partition by list(id)";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    int count = 10000;
    HoloMutation mutation;
    char buffer[10];
    for (int i = 0; i < count; i++) {
        sprintf(buffer, "%d", i/1000);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_val_with_text_by_colname(mutation, "id", buffer, strlen(buffer));
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_set_req_int32_val_by_colname(mutation, "name", i);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_put_051 where id2 = name + 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), count);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * 非常非常长的sql
 */
void testPut052() {
    // printf("---------testPut052--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_put_052";
    char* dropSql = "drop table if exists holo_client_put_052";
    char createSql[20568];
    int pos = 0;
    char* s1 = "create table holo_client_put_052 (id int not null";
    strcpy(createSql, s1); pos += strlen(s1);
    char s2[25];
    for (int i = 0; i < 820; i++) {
        sprintf(s2, ", name_name_name%d text", i);
        strcpy(createSql+pos, s2);
        pos += strlen(s2);
    }
    char* s3 = ",primary key(id))";
    strcpy(createSql+pos, s3);

    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0", 5);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 8);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from holo_client_put_052 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * writeMaxIntervalMs <= 0，不检查writeMaxIntervalMs
 */
void testPut053() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.batchSize = 10;
    config.writeMaxIntervalMs = -1;
    config.autoFlush = true;
    config.shardCollectorSize = config.threadSize;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    int ret;

    char* tableName = "holo_client_put_053";
    char* dropSql = "drop table if exists holo_client_put_053";
    char* createSql = "create table holo_client_put_053 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
    ret = holo_client_submit(client, mutation);
    CU_ASSERT_EQUAL(ret, HOLO_CLIENT_RET_OK);

    sleep(5);

    res = PQexec(conn, "select count(*) from holo_client_put_053");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 0), "0");
    PQclear(res);

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_put_053");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 0), "1");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

/**
 * 写分区表，攒批到一半，父表add column
 */
void testPut054() {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.batchSize = 10;
    config.writeMaxIntervalMs = -1;
    config.autoFlush = false;
    config.shardCollectorSize = config.threadSize;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    int ret;

    char* tableName = "holo_client_put_054";
    char* dropSql = "drop table if exists holo_client_put_054";
    char* createSql = "begin; create table holo_client_put_054 (id text not null, id2 int not null, name text, primary key(id,id2)) partition by list(id); create table holo_client_put_054_1 partition of holo_client_put_054 for values in ('1'); create table holo_client_put_054_2 partition of holo_client_put_054 for values in ('2'); commit;";
    char* addColumnSql = "alter table holo_client_put_054 add column added text";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "1", 1);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "name1", 4);
    ret = holo_client_submit(client, mutation);
    CU_ASSERT_EQUAL(ret, HOLO_CLIENT_RET_OK);

    PQclear(PQexec(conn, addColumnSql));

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_val_with_text_by_colindex(mutation, 0, "2", 1);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "name2", 4);
    ret = holo_client_submit(client, mutation);
    CU_ASSERT_EQUAL(ret, HOLO_CLIENT_PARTITION_META_CHANGE);

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_put_054");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 0), "1");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

// int main(int argc, char** argv) {
//     holo_client_logger_open();
//     testPut001();
//     testPut002();
//     testPut003();
//     testPut004();
//     testPut005();
//     testPut006();
//     testPut007();
//     testPut010();
//     testPut011();
//     testPut012();
//     testPut013();
//     testPut014();
//     testPut015();
//     testPut017();
//     testPut018();
//     testPut021();
//     testPut022();
//     testPut024();
//     testPut025();
//     testPut026();
//     testPut027();
//     testPut031();
//     testPut036();
//     testPut037();
//     testPut038();
//     testPut040();
//     testPut042();
//     testPut043();
//     testPut044();
//     testPut047();
//     testPut048();
//     testPut049();
//     testPut050();
//     testPut051();
//     testPut052();
//     holo_client_logger_close();
// }
