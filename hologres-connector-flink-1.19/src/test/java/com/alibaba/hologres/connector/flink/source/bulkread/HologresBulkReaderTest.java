package com.alibaba.hologres.connector.flink.source.bulkread;

import com.alibaba.hologres.org.postgresql.jdbc.PgConnection;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.SQLException;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for the connection handling of {@link HologresBulkReader}. */
public class HologresBulkReaderTest {

    private static HologresBulkReader newReader() {
        return new HologresBulkReader(null, null, null, null, null, null, false, null, -1);
    }

    @Test
    public void testCloseReleasesConnection() throws Exception {
        HologresBulkReader reader = newReader();
        PgConnection conn = mock(PgConnection.class);
        setConn(reader, conn);

        reader.close();

        verify(conn).close();
        assertNull(getConn(reader));
    }

    /**
     * Flink closes the input format from the finally block that rethrows the job failure, so a
     * failure while closing must not escape and replace the real root cause.
     */
    @Test
    public void testCloseSwallowsConnectionFailure() throws Exception {
        HologresBulkReader reader = newReader();
        PgConnection conn = mock(PgConnection.class);
        doThrow(new SQLException("connection close failed")).when(conn).close();
        setConn(reader, conn);

        reader.close();

        verify(conn).close();
        assertNull(getConn(reader));
    }

    /**
     * Flink also closes the input format after a failed open, which already released everything.
     */
    @Test
    public void testCloseIsIdempotentAndSafeWithoutOpen() throws Exception {
        HologresBulkReader reader = newReader();
        PgConnection conn = mock(PgConnection.class);
        setConn(reader, conn);

        reader.close();
        reader.close();

        verify(conn, times(1)).close();
        newReader().close();
    }

    /** A failed open() must not leave the connection behind for socketTimeout to clean up. */
    @Test
    public void testFailedOpenClosesConnection() throws Exception {
        HologresBulkReader reader = newReader();
        PgConnection conn = mock(PgConnection.class);
        setConn(reader, conn);

        Method closeConnection = HologresBulkReader.class.getDeclaredMethod("closeConnection");
        closeConnection.setAccessible(true);
        closeConnection.invoke(reader);

        verify(conn).close();
        assertNull(getConn(reader));
    }

    private static void setConn(HologresBulkReader reader, PgConnection conn) throws Exception {
        Field field = HologresBulkReader.class.getDeclaredField("conn");
        field.setAccessible(true);
        field.set(reader, conn);
    }

    private static Object getConn(HologresBulkReader reader) throws Exception {
        Field field = HologresBulkReader.class.getDeclaredField("conn");
        field.setAccessible(true);
        return field.get(reader);
    }
}
