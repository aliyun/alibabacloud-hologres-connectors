#include "holo_client.h"
#include "table_schema.h"
#include "holo_config.h"
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <CUnit/Basic.h>

#define MALLOC(n, type) \
    ((type *)malloc((n) * sizeof(type)))

typedef struct _TableInfo {
    HoloClient* client;
    char* tableName;
    HoloTableSchema* schema;
    int start;
} TableInfo;

void* put_record(void* arg) {
    TableInfo* info = arg;
    HoloTableSchema* schema = holo_client_get_tableschema(info->client, NULL, info->tableName, true); //tableSchema可以同时拿
    for (int i = info->start; i < info->start + 10000; i++) {
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
    holo_client_submit(info->client, mutation);
    }
    return NULL;
}

void* put_record_2(void* arg) {
    TableInfo* info = arg;
    HoloTableSchema* schema = holo_client_get_tableschema(info->client, NULL, info->tableName, true); //tableSchema可以同时拿
    for (int i = info->start; i < info->start + 10000; i++) {
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
    holo_client_set_req_int32_val_by_colindex(mutation, 1, 20240101);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
    holo_client_submit(info->client, mutation);
    }
    return NULL;
}

char* connInfo;


//insert into same table
void testMT1() {
    // printf("---------testMT001--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_mt_001";
    char* dropSql = "drop table if exists holo_client_mt_001";
    char* createSql = "create table holo_client_mt_001 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    TableInfo* info1 = MALLOC(1, TableInfo);
    info1->client = client;
    info1->tableName = tableName;
    info1->start = 0;
    TableInfo* info2 = MALLOC(1, TableInfo);
    info2->client = client;
    info2->tableName = tableName;
    info2->start = 10000;
    TableInfo* info3 = MALLOC(1, TableInfo);
    info3->client = client;
    info3->tableName = tableName;
    info3->start = 20000;

    pthread_t p1, p2, p3;
    pthread_create(&p1, NULL, put_record, info1);
    pthread_create(&p2, NULL, put_record, info2);
    pthread_create(&p3, NULL, put_record, info3);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(p3, NULL);

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_mt_001");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 30000);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
    free(info1);
    free(info2);
    free(info3);
}

//insert into different table
void testMT2() {
    // printf("---------testMT002--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName1 = "holo_client_mt_002_1";
    char* tableName2 = "holo_client_mt_002_2";
    char* tableName3 = "holo_client_mt_002_3";
    char* dropSql = "drop table if exists holo_client_mt_002_1;drop table if exists holo_client_mt_002_2;drop table if exists holo_client_mt_002_3";
    char* createSql = "create table holo_client_mt_002_1 (id int not null,name text,address text,primary key(id));create table holo_client_mt_002_2 (id int not null,name text,address text,primary key(id));create table holo_client_mt_002_3 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    TableInfo* info1 = MALLOC(1, TableInfo);
    info1->client = client;
    info1->tableName = tableName1;
    info1->start = 0;
    TableInfo* info2 = MALLOC(1, TableInfo);
    info2->client = client;
    info2->tableName = tableName2;
    info2->start = 0;
    TableInfo* info3 = MALLOC(1, TableInfo);
    info3->client = client;
    info3->tableName = tableName3;
    info3->start = 0;

    pthread_t p1, p2, p3;
    pthread_create(&p1, NULL, put_record, info1);
    pthread_create(&p2, NULL, put_record, info2);
    pthread_create(&p3, NULL, put_record, info3);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(p3, NULL);

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_mt_002_1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 10000);
    PQclear(res);
    res = PQexec(conn, "select count(*) from holo_client_mt_002_2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 10000);
    PQclear(res);
    res = PQexec(conn, "select count(*) from holo_client_mt_002_3");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 10000);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
    free(info1);
    free(info2);
    free(info3);
}

//insert into text partition parent table
void testMT3() {
    // printf("---------testMT003--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_mt_003";
    char* dropSql = "drop table if exists holo_client_mt_003";
    char* createSql = "create table holo_client_mt_003 (id int not null,name text,address text,primary key(id, name)) partition by list(name); create table holo_client_mt_003_name partition of holo_client_mt_003 for values in ('name');";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    TableInfo* info1 = MALLOC(1, TableInfo);
    info1->client = client;
    info1->tableName = tableName;
    info1->start = 0;
    TableInfo* info2 = MALLOC(1, TableInfo);
    info2->client = client;
    info2->tableName = tableName;
    info2->start = 10000;
    TableInfo* info3 = MALLOC(1, TableInfo);
    info3->client = client;
    info3->tableName = tableName;
    info3->start = 20000;

    pthread_t p1, p2, p3;
    pthread_create(&p1, NULL, put_record, info1);
    pthread_create(&p2, NULL, put_record, info2);
    pthread_create(&p3, NULL, put_record, info3);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(p3, NULL);

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_mt_003");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 30000);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
    free(info1);
    free(info2);
    free(info3);
}

//insert into int partition parent table
void testMT4() {
    // printf("---------testMT003--------------\n");
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    PGconn * conn = PQconnectdb(connInfo);
    PGresult* res;
    char* tableName = "holo_client_mt_004";
    char* dropSql = "drop table if exists holo_client_mt_004";
    char* createSql = "create table holo_client_mt_004 (id int not null,date int,address text,primary key(id, date)) partition by list(date); create table holo_client_mt_004_20240101 partition of holo_client_mt_004 for values in (20240101);";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    TableInfo* info1 = MALLOC(1, TableInfo);
    info1->client = client;
    info1->tableName = tableName;
    info1->start = 0;
    TableInfo* info2 = MALLOC(1, TableInfo);
    info2->client = client;
    info2->tableName = tableName;
    info2->start = 10000;
    TableInfo* info3 = MALLOC(1, TableInfo);
    info3->client = client;
    info3->tableName = tableName;
    info3->start = 20000;

    pthread_t p1, p2, p3;
    pthread_create(&p1, NULL, put_record_2, info1);
    pthread_create(&p2, NULL, put_record_2, info2);
    pthread_create(&p3, NULL, put_record_2, info3);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(p3, NULL);

    holo_client_flush_client(client);

    res = PQexec(conn, "select count(*) from holo_client_mt_004");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 30000);
    PQclear(res);

    PQfinish(conn);
    holo_client_close_client(client);
    free(info1);
    free(info2);
    free(info3);
}

// int main(int argc, char** argv) {
//     holo_client_logger_open();
//     testMT1();
//     testMT2();
//     holo_client_logger_close();
//     return 0;
// }