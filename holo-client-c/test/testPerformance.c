#include "holo_client.h"
#include "table_schema.h"
#include "holo_config.h"
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <CUnit/Basic.h>

char* connInfo;

/*
 * 单线程写入性能测试
 */
void testPerformance01(void) {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.batchSize = 512;
    config.threadSize = 3;
    PGconn * conn = PQconnectdb(connInfo);

    char* tableName = "holo_client_performance_001";
    char* dropSql = "drop table if exists holo_client_performance_001";
    char* createSql = "create table holo_client_performance_001 (id int not null,name text,address text,primary key(id))";
    PQclear(PQexec(conn, dropSql));
    PQclear(PQexec(conn, createSql));
    HoloClient* client = holo_client_new_client(config);

    HoloTableSchema* schema = holo_client_get_tableschema(client, NULL, tableName, true);
    for (int i = 0; i < 100000000; i++) {
        HoloMutation mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        holo_client_set_req_val_with_text_by_colindex(mutation, 1, "name", 4);
        holo_client_set_req_val_with_text_by_colindex(mutation, 2, "address", 7);
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    PQfinish(conn);
    holo_client_close_client(client);
}

/*
 * 单线程写入性能测试，宽表
 */
void testPerformance02(void) {
    HoloConfig config = holo_client_new_config(connInfo);
    config.writeMode = INSERT_OR_REPLACE;
    config.batchSize = 512;
    config.threadSize = 3;
    PGconn * conn = PQconnectdb(connInfo);

    char* tableName = "holo_client_performance_002";
    char* dropSql = "drop table if exists holo_client_performance_002";
    char createSql[21000];
    int nCol = 100;
    int pos = 0;
    char* s1 = "create table holo_client_performance_002 (id int not null";
    strcpy(createSql, s1); pos += strlen(s1);
    char s2[25];
    for (int i = 0; i < nCol - 1; i++) {
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
    for (int i = 0; i < 100000000; i++) {
        HoloMutation mutation = holo_client_new_mutation_request(schema);
        holo_client_set_req_int32_val_by_colindex(mutation, 0, i);
        for (int j = 1; j <= nCol - 1; j++) {
            holo_client_set_req_val_with_text_by_colindex(mutation, j, "name", 4);
        }
        holo_client_submit(client, mutation);
    }
    holo_client_flush_client(client);

    PQfinish(conn);
    holo_client_close_client(client);
}

// int main(int argc, char** argv) {
//     holo_client_logger_open();
//     testPerformance01();
//     testPerformance02();
//     holo_client_logger_close();
//     return 0;
// }