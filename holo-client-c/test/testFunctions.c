#include "table_schema.h"
#include "table_schema_private.h"
#include "utils.h"
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <CUnit/Basic.h>

extern int unnest_convert_array_to_text(char**, char**, int*, int);

/*
 * 测试quote_table_name函数
 * 功能: 对于包含大写和特殊字符的输入，前后加"。对于输入中的"，转义为""
 * 输入: 用户输入holo_client_get_tableschema()的schema名和table名，大小写敏感
 * 输出: 用来拼接INSERT，DELETE，SELECT的sql，作为PG标识符
 */
void testQuoteTableName(void) {
    const char* schemaName[] = {"test_schema", "\"test_ Schema\\',?"};

    const char* tableName[] = {"test_table",
                               "test_Table",
                               "test_\"table",
                               "\"test_table",
                               "test_'table",
                               "test_\'table",
                               "test_\\table",
                               "test_\?table",
                               "test_     table",
                               "test_'aaaA'table",
                               "test_\'aaaA\'table",
                               "test_\"aaaA\"table",
                               "test_,table"
                               };

    const char* expectResult[] = {"test_schema.test_table",
                                  "test_schema.\"test_Table\"",
                                  "test_schema.\"test_\"\"table\"",
                                  "test_schema.\"\"\"test_table\"",
                                  "test_schema.\"test_'table\"",
                                  "test_schema.\"test_\'table\"",
                                  "test_schema.\"test_\\table\"",
                                  "test_schema.\"test_\?table\"",
                                  "test_schema.\"test_     table\"",
                                  "test_schema.\"test_'aaaA'table\"",
                                  "test_schema.\"test_\'aaaA\'table\"",
                                  "test_schema.\"test_\"\"aaaA\"\"table\"",
                                  "test_schema.\"test_,table\""
                                  };

    // check several cases of table name
    for (int i = 0; i < 13; i++) {
        char* quoted = quote_table_name(schemaName[0], tableName[i]);
        CU_ASSERT_STRING_EQUAL(quoted, expectResult[i]);
        FREE(quoted);
    }

    // check both schema name and table name
    char* quoted = quote_table_name(schemaName[1], tableName[1]);
    CU_ASSERT_STRING_EQUAL(quoted, "\"\"\"test_ Schema\\',?\".\"test_Table\"");
    FREE(quoted);

    // check PG RESERVED_KEYWORD and UNRESERVED_KEYWORD
    char* quotedKeywords = quote_table_name("user", "database");
    CU_ASSERT_STRING_EQUAL(quotedKeywords, "\"user\".database");
    FREE(quotedKeywords);
}

/*
 * 测试quote_literal_cstr函数
 * 功能: 对于输入，前后加'。对于输入中的'和\做转义
 * 输入: 用户输入holo_client_get_tableschema()的schema名和table名，大小写敏感
 * 输出: 用来在创建动态分区时拼接建分区表的sql，作为PG字符串
 */
void testQuoteLiteralCStr(void) {
    const char* text[] = {"test_table",
                            "test_Table",
                            "test_\"table",
                            "\"test_table",
                            "test_'table",
                            "test_\'table",
                            "test_\\table",
                            "test_\?table",
                            "test_     table",
                            "test_'aaaA'table",
                            "test_\'aaaA\'table",
                            "test_\"aaaA\"table",
                            "test_,table"
                            };

    const char* expectResult[] = {"\'test_table\'",
                                  "\'test_Table\'",
                                  "\'test_\"table\'",
                                  "\'\"test_table\'",
                                  "\'test_\'\'table\'",
                                  "\'test_\'\'table\'",
                                  "E\'test_\\\\table\'",
                                  "\'test_\?table\'",
                                  "\'test_     table\'",
                                  "\'test_\'\'aaaA\'\'table\'",
                                  "\'test_\'\'aaaA\'\'table\'",
                                  "\'test_\"aaaA\"table\'",
                                  "\'test_,table\'"
                                  };

    // check several cases of text
    for (int i = 0; i < 13; i++) {
        char* quoted = quote_literal_cstr(text[i]);
        CU_ASSERT_STRING_EQUAL(quoted, expectResult[i]);
        FREE(quoted);
    }
}

/*
 * 测试unnest_convert_array_to_text函数
 * 功能: unnest模式下，将record->value组成的char*数组拼接成PG的数组字符串，同时对\做转义
 * 输入: 目标字符串的指针，record->value组成的char*数组，record数
 * 输出: 输出PG数组字符串的长度
 */
void testUnnestConvertArrayToText(void) {
    int length = 0;
    char* ptr = NULL;
    char** valueArray = MALLOC(3, char*);
    int* lenArray = MALLOC(3, int);

    // normal case
    valueArray[0] = "name1";
    valueArray[1] = "name2";
    valueArray[2] = "name3";
    lenArray[0] = 6;
    lenArray[1] = 6;
    lenArray[2] = 6;
    length = unnest_convert_array_to_text(&ptr, valueArray, lenArray, 3);
    CU_ASSERT_STRING_EQUAL(ptr, "{\"name1\",\"name2\",\"name3\"}");
    CU_ASSERT_EQUAL(length, 26);
    FREE(ptr);

    // 有NULL
    valueArray[0] = NULL;
    valueArray[1] = NULL;
    valueArray[2] = "name3";
    lenArray[0] = 0;
    lenArray[1] = 0;
    lenArray[2] = 6;
    length = unnest_convert_array_to_text(&ptr, valueArray, lenArray, 3);
    CU_ASSERT_STRING_EQUAL(ptr, "{NULL,NULL,\"name3\"}");
    CU_ASSERT_EQUAL(length, 20);
    FREE(ptr);

    // 有双引号
    valueArray[0] = "\"name1\"";
    valueArray[1] = "na\"me2";
    valueArray[2] = "na'me3'";
    lenArray[0] = 8;
    lenArray[1] = 7;
    lenArray[2] = 8;
    length = unnest_convert_array_to_text(&ptr, valueArray, lenArray, 3);
    CU_ASSERT_STRING_EQUAL(ptr, "{\"\\\"name1\\\"\",\"na\\\"me2\",\"na'me3'\"}");
    CU_ASSERT_EQUAL(length, 34);
    FREE(ptr);

    // 有转义符
    valueArray[0] = "na\'me1";
    valueArray[1] = "na\?me2";
    valueArray[2] = "na\\me3";
    lenArray[0] = 7;
    lenArray[1] = 7;
    lenArray[2] = 7;
    length = unnest_convert_array_to_text(&ptr, valueArray, lenArray, 3);
    CU_ASSERT_STRING_EQUAL(ptr, "{\"na\'me1\",\"na\?me2\",\"na\\\\me3\"}");
    CU_ASSERT_EQUAL(length, 30);
    FREE(ptr);

    // 有\0
    valueArray[0] = "name1";
    valueArray[1] = "name2";
    valueArray[2] = "na\0me3";
    lenArray[0] = 6;
    lenArray[1] = 6;
    lenArray[2] = 3;
    length = unnest_convert_array_to_text(&ptr, valueArray, lenArray, 3);
    CU_ASSERT_STRING_EQUAL(ptr, "{\"name1\",\"name2\",\"na\"}"); // \0会被截断
    CU_ASSERT_EQUAL(length, 23);
    FREE(ptr);

    FREE(valueArray);
    FREE(lenArray);
}

void testGetColumnInfo(void) {
    HoloTableSchema* schema = holo_client_new_tableschema();
    schema->nColumns = 3;
    HoloColumn* columns = holo_client_new_columns(3);
    columns[0].name = deep_copy_string("c1");
    columns[0].type = HOLO_TYPE_INT4;
    columns[1].name = deep_copy_string("C2");
    columns[1].type = HOLO_TYPE_TEXT;
    columns[2].name = deep_copy_string("c3");
    columns[2].type = -1; // invalid oid
    schema->columns = columns;

    // 测试holo_client_get_column_name
    CU_ASSERT_EQUAL(holo_client_get_column_name(NULL, 0), NULL);
    CU_ASSERT_EQUAL(holo_client_get_column_name(schema, 3), NULL);
    CU_ASSERT_STRING_EQUAL(holo_client_get_column_name(schema, 0), "c1");
    CU_ASSERT_STRING_EQUAL(holo_client_get_column_name(schema, 1), "C2");
    CU_ASSERT_STRING_EQUAL(holo_client_get_column_name(schema, 2), "c3");

    // 测试holo_client_get_column_type_name
    CU_ASSERT_EQUAL(holo_client_get_column_type_name(NULL, 0), NULL);
    CU_ASSERT_EQUAL(holo_client_get_column_type_name(schema, 3), NULL);
    CU_ASSERT_STRING_EQUAL(holo_client_get_column_type_name(schema, 0), "int4");
    CU_ASSERT_STRING_EQUAL(holo_client_get_column_type_name(schema, 1), "text");
    CU_ASSERT_EQUAL(holo_client_get_column_type_name(schema, 2), NULL);

    holo_client_destroy_tableschema(schema);
}