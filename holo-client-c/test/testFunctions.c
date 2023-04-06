#include "utils.h"
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <CUnit/Basic.h>

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