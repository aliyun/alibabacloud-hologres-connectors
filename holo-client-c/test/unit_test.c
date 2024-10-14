#include "holo_client.h"
#include "table_schema.h"
#include "holo_config.h"
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <CUnit/Basic.h>
#include <assert.h>

char* connInfo = "host=test_instance_host port=test_instance_port dbname=test_db user=test_user password=test_password";


#include "testHoloClient.c"
#include "testDataType.c"
#include "testMultiThread.c"
#include "testPerformance.c"
#include "testFunctions.c"

CU_TestInfo testcase[] = {
    {"testConnectFail", testConnectFail},
    {"testGet", testGet},
    {"testPut001", testPut001},
    {"testPut002", testPut002},
    {"testPut003", testPut003},
    {"testPut004", testPut004},
    {"testPut005", testPut005},
    {"testPut006", testPut006},
    {"testPut007", testPut007},
    {"testPut008", testPut008},
    {"testPut009", testPut009},
    {"testPut010", testPut010},
    {"testPut011", testPut011},
    {"testPut012", testPut012},
    {"testPut013", testPut013},
    {"testPut014", testPut014},
    {"testPut015", testPut015},
    {"testPut016", testPut016},
    {"testPut017", testPut017},
    {"testPut018", testPut018},
    {"testPut019", testPut019},
    {"testPut020", testPut020},
    {"testPut021", testPut021},
    {"testPut022", testPut022},
    {"testPut024", testPut024},
    {"testPut025", testPut025},
    {"testPut026", testPut026},
    {"testPut027", testPut027},
    {"testPut031", testPut031},
    {"testPut036", testPut036},
    {"testPut037", testPut037},
    {"testPut038", testPut038},
    {"testPut040", testPut040},
    {"testPut042", testPut042},
    {"testPut043", testPut043},
    {"testPut044", testPut044},
    {"testPut047", testPut047},
    {"testPut048", testPut048},
    {"testPut049", testPut049},
    {"testPut050", testPut050},
    {"testPut051", testPut051},
    {"testPut052", testPut052},
    {"testPut053", testPut053},
    {"testPut054", testPut054},
    CU_TEST_INFO_NULL
};

CU_TestInfo testcase_type[] = {
    {"testBasicTypes", testBasicTypes},
    {"testDecimal", testDecimal},
    {"testArrayTypes", testArrayTypes},
    {"testTimestamp", testTimestamp},
    {"testOtherTypes", testOtherTypes},
    {"testOtherTypes2", testOtherTypes2},
    CU_TEST_INFO_NULL
};

CU_TestInfo testcase_multithread[] = {
    {"testMultiThread01", testMT1},
    {"testMultiThread02", testMT2},
    {"testMultiThread03", testMT3},
    {"testMultiThread03", testMT4},
    CU_TEST_INFO_NULL
};

CU_TestInfo testcase_performance[] = {
    {"testPerformance01", testPerformance01},
    {"testPerformance02", testPerformance02},
    CU_TEST_INFO_NULL
};

CU_TestInfo testcase_functions[] = {
    {"testQuoteTableName", testQuoteTableName},
    {"testQuoteLiteralCStr", testQuoteLiteralCStr},
    {"testUnnestConvertArrayToText", testUnnestConvertArrayToText},
    {"testGetColumnInfo", testGetColumnInfo},
    CU_TEST_INFO_NULL
};

int suite_success_init(void) {
    return 0;
}

int suite_success_clean(void) {
    return 0;
}

CU_SuiteInfo suites[] = {
    {"testPutSuite", suite_success_init, suite_success_clean, NULL, NULL, testcase},
    {"testDataTypeSuite", suite_success_init, suite_success_clean, NULL, NULL, testcase_type},
    {"testMultiThreadSuite", suite_success_init, suite_success_clean, NULL, NULL, testcase_multithread},
    {"testFuncsSuite", suite_success_init, suite_success_clean, NULL, NULL, testcase_functions},
    // 性能测试默认不开启
    // {"testPerformanceSuite", suite_success_init, suite_success_clean, NULL, NULL, testcase_performance},
    CU_SUITE_INFO_NULL
};

void AddTests() {
    assert(NULL !=  CU_get_registry());
    assert(!CU_is_test_running());

    if(CUE_SUCCESS != CU_register_suites(suites)) {
        exit(EXIT_FAILURE);
    }
}

int RunTest() {
    holo_client_logger_open();
    if(CU_initialize_registry()) {
        fprintf(stderr, " Initialization of Test Registry failed. ");
        holo_client_logger_close();
        exit(EXIT_FAILURE);
    } else {
        AddTests();
        CU_basic_set_mode(CU_BRM_VERBOSE);
        CU_basic_run_tests();
        CU_cleanup_registry();
        holo_client_logger_close();
        return CU_get_error();
    }
}

int main(int argc, char** argv) {
    return RunTest();
}