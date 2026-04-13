package com.alibaba.hologres.client.copy.out;

import com.alibaba.hologres.client.Version;
import com.alibaba.hologres.client.copy.CopyContextCommon;
import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.out.arrow.ArrowReader;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CopyOutContext extends CopyContextCommon {
    public static final Logger LOG = LoggerFactory.getLogger(CopyOutContext.class);
    static Set<String> needClearMetaTypes =
            new HashSet<String>() {
                {
                    add("timetz");
                    add("bytea");
                    add("char");
                    add("bpchar");
                    add("varchar");
                    add("roaringbitmap");
                }
            };

    // for copy out
    public CopyOutInputStream outOs = null;
    public ArrowReader reader = null;

    List<Integer> shards;
    String filter;

    public CopyOutContext(
            Connection conn,
            TableSchema schema,
            List<String> columns,
            CopyFormat copyFormat,
            List<Integer> shards,
            String filter,
            int maxCellBufferSize) {
        super(conn, schema, columns, copyFormat, maxCellBufferSize);
        this.shards = shards;
        this.filter = filter;
    }

    @Override
    public void init() throws IOException {
        try {
            Set<String> jsonbColumns = new HashSet<>();
            boolean setClearMeta = false;
            for (String column : columns) {
                if (schema.getColumnIndex(column) == null) {
                    String tableName = schema.getTableNameObj().getFullName();
                    throw new IOException("column " + column + " not found in table " + tableName);
                }
                String typeName = schema.getColumn(schema.getColumnIndex(column)).getTypeName();
                if (typeName.equals("jsonb")) {
                    jsonbColumns.add(column);
                }
                if (needClearMetaTypes.contains(typeName)) {
                    setClearMeta = true;
                }
            }
            if (setClearMeta) {
                try {
                    HoloVersion holoVersion = ConnectionUtil.getHoloVersion(conn);
                    if (!isVersionValid(holoVersion)) {
                        throw new IOException(
                                String.format(
                                        "holo-client:%s copy out with arrow format has %s types need holo instance version >= r4.1.0 or r4.0.18, but current version is %s",
                                        Version.version, needClearMetaTypes, holoVersion));
                    }
                    // 当前版本将holo-client依赖的arrow
                    // sdk从0.10.0升级到了17.0.0,在解析holo返回的一些字段类型时(needClearMetaTypes)时,
                    // 可能无法解析holo自定义的metadata,因此需要特别设置以下guc,使holo在返回之前清除metadata
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("SET hg_experimental_copy_out_arrow_clear_fields_meta = true");
                    }
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
            String copySql =
                    CopyUtil.buildCopyOutSql(
                            schema.getTableNameObj().getFullName(),
                            columns,
                            shards,
                            copyFormat,
                            filter,
                            jsonbColumns);
            LOG.info("copy out sql: {}", copySql);
            copyManager = new CopyManager(conn.unwrap(PgConnection.class));
            outOs = new CopyOutInputStream(copyManager.copyOut(copySql), maxCellBufferSize);
            switch (copyFormat) {
                case ARROW:
                    reader = new ArrowReader(outOs, false);
                    break;
                case ARROW_LZ4:
                    reader = new ArrowReader(outOs, true);
                    break;
                default:
                    throw new RuntimeException("unsupported copy out format: " + copyFormat);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        isInit.set(true);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        copyManager = null;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void checkContextOpen() throws IOException {
        if (IsNotInitialized()) {
            init();
        }
        if (!isInit.get() || conn == null || outOs == null || reader == null) {
            throw new IOException("copy out context is not initialized");
        }
    }

    /** 新版本的arrow sdk必须设置guc才可以读取部分类型, 版本不满足的需要抛出异常 */
    private boolean isVersionValid(HoloVersion v) {
        // 4.1+ 全部合法
        if (v.compareTo(new HoloVersion(4, 1, 0)) >= 0) {
            return true;
        }
        // 4.0+ 仅支持 4.0.18+
        if (v.getMajorVersion() == 4 && v.getMinorVersion() == 0) {
            return v.compareTo(new HoloVersion(4, 0, 18)) >= 0;
        }
        return false;
    }
}
