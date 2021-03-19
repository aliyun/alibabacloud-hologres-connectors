package com.alibaba.ververica.connectors.hologres;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/** JDBCTestUtils. */
public class JDBCTestUtils {

    public static void checkResult(
            String[] expectedResult,
            String table,
            String[] fields,
            String url,
            String userName,
            String password)
            throws SQLException {

        try (Connection dbConn = DriverManager.getConnection(url, userName, password);
                PreparedStatement statement = dbConn.prepareStatement("select * from " + table);
                ResultSet resultSet = statement.executeQuery()) {
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                List<String> result = new ArrayList<>();
                for (int i = 0; i < fields.length; i++) {
                    result.add(resultSet.getObject(fields[i]).toString());
                }
                results.add(StringUtils.join(result, ","));
            }
            String[] sortedResult = results.toArray(new String[0]);
            Arrays.sort(expectedResult);
            Arrays.sort(sortedResult);
            assertArrayEquals(expectedResult, sortedResult);
        }
    }

    public static void checkResultWithTimeout(
            String[] expectedResult,
            String table,
            String[] fields,
            String url,
            String userName,
            String password,
            long timeout)
            throws SQLException, InterruptedException {

        long endTimeout = System.currentTimeMillis() + timeout;
        boolean result = false;
        while (System.currentTimeMillis() < endTimeout) {
            try {
                checkResult(expectedResult, table, fields, url, userName, password);
                result = true;
                break;
            } catch (AssertionError | SQLException throwable) {
                Thread.sleep(1000L);
            }
        }
        if (!result) {
            checkResult(expectedResult, table, fields, url, userName, password);
        }
    }
}
