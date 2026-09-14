package com.alibaba.hologres.connector.flink.source.bulkread;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;
import org.mockito.Answers;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for {@link HologresCopyBulkreadInputFormat}. */
public class HologresCopyBulkreadInputFormatTest {

    @Test
    public void testInputSplitAssigner() {
        HologresCopyBulkreadInputFormat inputFormat = newInputFormatWithoutConstructor();
        HologresShardInputSplit[] splits =
                new HologresShardInputSplit[] {
                    new HologresShardInputSplit(0), new HologresShardInputSplit(1)
                };
        assertTrue(inputFormat.getInputSplitAssigner(splits) instanceof DefaultInputSplitAssigner);
    }

    @Test
    public void testReachedEndCachesCurrentRecord() throws Exception {
        HologresCopyBulkreadInputFormat inputFormat = newInputFormatWithoutConstructor();
        HologresCopyBulkReader copyReader = mock(HologresCopyBulkReader.class);
        RowData expected = GenericRowData.of(1, "value");
        doReturn(expected, null).when(copyReader).nextRecord();
        setField(inputFormat, "copyReader", copyReader);

        assertTrue(!inputFormat.reachedEnd());
        assertSame(expected, inputFormat.nextRecord(null));
        assertTrue(inputFormat.reachedEnd());
        assertNull(inputFormat.nextRecord(null));
        verify(copyReader, times(2)).nextRecord();
    }

    @Test
    public void testCloseDelegatesAndIsSafeBeforeOpen() throws Exception {
        HologresCopyBulkreadInputFormat unopened = newInputFormatWithoutConstructor();
        unopened.close();

        HologresCopyBulkreadInputFormat opened = newInputFormatWithoutConstructor();
        HologresCopyBulkReader copyReader = mock(HologresCopyBulkReader.class);
        setField(opened, "copyReader", copyReader);
        opened.close();
        verify(copyReader).close();
    }

    @Test
    public void testNoOpMethods() throws IOException {
        HologresCopyBulkreadInputFormat inputFormat = newInputFormatWithoutConstructor();
        inputFormat.configure(new Configuration());
        assertNull(inputFormat.getStatistics(null));
    }

    private static HologresCopyBulkreadInputFormat newInputFormatWithoutConstructor() {
        return mock(HologresCopyBulkreadInputFormat.class, Answers.CALLS_REAL_METHODS);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = HologresCopyBulkreadInputFormat.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
