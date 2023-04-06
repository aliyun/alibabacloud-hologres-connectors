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
    for (int i = info->start; i < info->start + 1000; i++) {
    HoloMutation mutation = holo_client_new_mutation_request(info->schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
    holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
    holo_client_submit(info->client, mutation);
    }
    return NULL;
}

void* put_record_2(void* arg) {
    TableInfo* info = arg;
    HoloTableSchema* schema = holo_client_get_tableschema(info->client, NULL, info->tableName, true); //不同的表可以同时拿
    for (int i = info->start; i < info->start + 1000; i++) {
    HoloMutation mutation = holo_client_new_mutation_request(schema);
    holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
    holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
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
    //同一张表要在主线程拿table schema, 或者不同时拿，多线程同时拿的话会出问题
    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);

    TableInfo* info1 = MALLOC(1, TableInfo);
    info1->client = client;
    info1->schema = schema;
    info1->start = 0;
    TableInfo* info2 = MALLOC(1, TableInfo);
    info2->client = client;
    info2->schema = schema;
    info2->start = 1000;
    TableInfo* info3 = MALLOC(1, TableInfo);
    info3->client = client;
    info3->schema = schema;
    info3->start = 2000;

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
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 3000);
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
    char* tableName1 = MALLOC(25, char);
    sprintf(tableName1, "%s", "holo_client_mt_002_1");
    char* tableName2 = MALLOC(25, char);
    sprintf(tableName2, "%s", "holo_client_mt_002_2");
    char* tableName3 = MALLOC(25, char);
    sprintf(tableName3, "%s", "holo_client_mt_002_3");
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
    pthread_create(&p1, NULL, put_record_2, info1);
    pthread_create(&p2, NULL, put_record_2, info2);
    pthread_create(&p3, NULL, put_record_2, info3);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(p3, NULL);

    holo_client_flush_client(client);


    res = PQexec(conn, "select count(*) from holo_client_mt_002_1");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 1000);
    PQclear(res);
    res = PQexec(conn, "select count(*) from holo_client_mt_002_2");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 1000);
    PQclear(res);
    res = PQexec(conn, "select count(*) from holo_client_mt_002_3");
    CU_ASSERT_NOT_EQUAL(PQntuples(res), 0);
    CU_ASSERT_EQUAL(atoi(PQgetvalue(res, 0, 0)), 1000);
    PQclear(res);


    PQfinish(conn);
    holo_client_close_client(client);
    free(info1);
    free(info2);
    free(info3);
    free(tableName1);
    free(tableName2);
    free(tableName3);
}

// int main(int argc, char** argv) {
//     holo_client_logger_open();
//     testMT1();
//     testMT2();
//     holo_client_logger_close();
//     return 0;
// }