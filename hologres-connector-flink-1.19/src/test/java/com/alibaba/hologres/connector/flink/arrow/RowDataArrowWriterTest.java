package com.alibaba.hologres.connector.flink.arrow;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.connector.flink.sink.v2.stage.StageHelper;
import org.junit.Test;

import java.sql.Types;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for Stage Arrow writer options. */
public class RowDataArrowWriterTest {

    @Test
    public void testCompressionFlagIsPassedToArrowWriter() throws Exception {
        TableSchema schema = createSingleTextColumnSchema();
        RowType rowType = RowType.of(new LogicalType[] {new VarCharType()}, new String[] {"value"});

        try (RowDataArrowWriter uncompressed =
                        new RowDataArrowWriter(
                                schema, Collections.singletonList("value"), 128, rowType, false);
                RowDataArrowWriter compressed =
                        new RowDataArrowWriter(
                                schema, Collections.singletonList("value"), 128, rowType, true)) {
            assertFalse(uncompressed.isCompressionEnabled());
            assertTrue(compressed.isCompressionEnabled());
        }
    }

    @Test
    public void testArrowWriterRejectsMissingTargetColumn() {
        TableSchema schema = createSingleTextColumnSchema();

        try {
            StageHelper.validateStageColumns(
                    schema, Collections.singletonList("missing_column"), "test_table");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("missing_column"));
            assertTrue(e.getMessage().contains("test_table"));
        }
    }

    private static TableSchema createSingleTextColumnSchema() {
        Column column = new Column();
        column.setName("value");
        column.setType(Types.VARCHAR);
        column.setTypeName("text");
        column.setAllowNull(true);
        TableSchema schema = new TableSchema.Builder().addColumn(column).build();
        schema.calculateProperties();
        return schema;
    }
}
