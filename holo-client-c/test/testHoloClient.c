#include "holo_client.h"
#include "table_schema.h"
#include "holo_config.h"
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <CUnit/Basic.h>

char* connInfo;

/**
 * INSERT_REPLACE.
 */
/**
 * INSERT_REPLACE.
 */
void testPut001(void) {
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

    TableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name3");
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

    TableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name3");
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

    TableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address3");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address3");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_005 where id = 0 and name = 'name0'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
}

void* print_failed_record(Record* record, char* errMsg) {
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    printf("-----------should have two failed put------------\n");

    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql1));

    schema = holo_client_get_tableschema(client, "test_schema", tableName, false);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
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
 * CUSTOM_SCHEMA，tableName. columnName.
 */
void testPut007() {
    // printf("---------testPut007--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_IGNORE;
    config.dynamicPartition = true;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holO_client_put_007";
    char* createSchema = "create schema if not exists test_schema";
    char* dropSql = "drop table if exists test_schema.\"holO_client_put_007\"";
    char* createSql = "create table test_schema.\"holO_client_put_007\" (id int not null,\"nAme\" text,address text,primary key(id))";
    PQclear(PQexec(conn, createSchema));
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.\"holO_client_put_007\" where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    res = PQexec(conn, "select * from test_schema.\"holO_client_put_007\" where id = 1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name1");
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address2");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    CU_ASSERT_STRING_EQUAL(schema->columns[1].name, "a\"b");

    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    CU_ASSERT_STRING_EQUAL(schema->columns[1].name, "a\"b");

    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name2");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, NULL);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 3);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name3");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address3");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    CU_ASSERT_STRING_EQUAL(schema->columns[1].name, "a\"b");

    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    CU_ASSERT_STRING_EQUAL(schema->columns[1].name, "a\"b");

    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    sleep(10);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    sleep(5);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    sleep(5);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 2);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name2");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 3);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name3");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 4);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "address2");
    holo_client_submit(client, mutation);
    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colname(mutation, "id", 1);
    holo_client_set_req_val_with_text_by_colname(mutation, "a\"b", "name1");
    holo_client_set_req_val_with_text_by_colname(mutation, "address", "ccc");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0_new");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0_new");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    bool success = true;
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, NULL);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    //等待超过WriteMaxIntervalMs时间后自动提交
    sleep(5);

    printf("-----------should have one failed put------------\n");\

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name2");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int64_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address0");
    holo_client_submit(client, mutation);

    PQclear(PQexec(conn, addColumnSql));

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int64_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name1");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    schema = holo_client_get_tableschema(client, "test_schema", tableName, false);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int64_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name2");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address2");
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "new");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    TableSchema* schema1 = holo_client_get_tableschema(client, "test_schema", "holo_client_put_022_1", true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2017-05-01 12:10:55");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema1);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "201705 12:10:55");
    holo_client_submit(client, mutation);
    holo_client_flush_client(client);

    printf("-----------should have one failed put------------\n");\


    res = PQexec(conn, "select count(*) from test_schema.holo_client_put_022");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 1);
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "a");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "b");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "b");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "c");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "123");
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
 * partition int
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);

    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "123");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa");
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "123");
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
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa");
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "123");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 12);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "aaa");
    holo_client_set_req_val_with_text_by_colindex(mutation, 3, "123");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 2);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "456");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2019-01-02");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address1");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    res = PQexec(conn, "select * from test_schema.holo_client_put_031 where id = 0 and name = '2019-01-02'");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 2), "address1");
    PQclear(res);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "2019-01-02");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "abc");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, -1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name-1");
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
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer);
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, -1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name-1");
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
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer);
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, -1);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name-1");
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
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer);
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    int count = 100000;
    Mutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_set_req_int32_val_by_colindex(mutation, 1, i+1);
        holo_client_set_req_val_with_text_by_colindex(mutation, 2, "name-1");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "16.211");
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 1);
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);


    bool success = true;
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "16.211");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_request_mode(mutation, DELETE);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_submit(client, mutation);

    mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "19.211");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    Mutation mutation;
    char buffer[10];
    for (int i = 0; i < 100; i++) {
        sprintf(buffer, "%d", i);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer);
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
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer);
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
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer);
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
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, buffer);
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    int count = 10000;
    Mutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i/1000);
        holo_client_set_req_int32_val_by_colindex(mutation, 1, i+1);
        holo_client_set_req_val_with_text_by_colindex(mutation, 2, "name-1");
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

    TableSchema* schema = holo_client_get_tableschema(client, "test_schema", tableName, true);
    int count = 10000;
    Mutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colname(mutation, "id", i/1000);
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_set_req_val_with_text_by_colname(mutation, "name", "name-1");
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

    TableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    int count = 10000;
    Mutation mutation;
    for (int i = 0; i < count; i++) {
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colname(mutation, "id", i/1000);
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_set_req_val_with_text_by_colname(mutation, "name", "name-1");
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

    TableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    int count = 100000;
    Mutation mutation;
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

    TableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    int count = 10000;
    Mutation mutation;
    char buffer[10];
    for (int i = 0; i < count; i++) {
        sprintf(buffer, "%d", i/1000);
        mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_val_with_text_by_colname(mutation, "id", buffer);
        holo_client_set_req_int32_val_by_colname(mutation, "id2", i+1);
        holo_client_set_req_int32_val_by_colname(mutation, "name", i);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    bool success = true;
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

    TableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    Mutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, 0);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name0");
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address");
    holo_client_submit(client, mutation);

    holo_client_flush_client(client);


    res = PQexec(conn, "select * from holo_client_put_052 where id = 0");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_STRING_EQUAL(PQgetvalue(res, 0, 1), "name0");
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
