package com.alibaba.hologres.performace.client;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

public class SqlUtil {
  public static Logger LOG = LoggerFactory.getLogger(SqlUtil.class);

  public static void createTable(Connection conn, boolean partition, String tableName,
      int columnCount, int shardCount, String orientation, boolean enableBinlog,
      boolean enableBitmap, boolean additionTsColumn, boolean hasPk, boolean prefixPk , String dataColumnType) throws
      SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("begin;\ndrop table if exists ")
        .append(tableName)
        .append(";\ncreate table ")
        .append(tableName)
        .append("(id int");
    if (prefixPk) {
      sb.append(",id1 int");
    }
    for (int i = 0; i < columnCount; ++i) {
      sb.append(",name").append(i).append(" ").append(dataColumnType);
    }
    if (additionTsColumn) {
      sb.append(",ts").append(" timestamptz not null");
    }
    if (partition) {
      sb.append(", ds int not null");
    }
    if (hasPk) {
      sb.append(",primary key(id");
      if (prefixPk) {
        sb.append(",id1");
      }
      if (partition) {
        sb.append(",ds");
      }
      sb.append(")");
    }
    sb.append(")");
    if (partition) {
      sb.append("partition by list(ds)");
    }
    sb.append(";\n");
    if (!hasPk || (hasPk && prefixPk)) {
      sb.append("call set_table_property('")
          .append(tableName)
          .append("','distribution_key','")
          .append("id")
          .append("');\n");
    }
    sb.append("call set_table_property('")
        .append(tableName)
        .append("','orientation','")
        .append(orientation)
        .append("');\n");
    if (shardCount > 0) {
      sb.append("call set_table_property('")
          .append(tableName)
          .append("','shard_count','")
          .append(shardCount)
          .append("');\n");
    }
    if (enableBinlog) {
      sb.append("call set_table_property('")
          .append(tableName)
          .append("','clustering_key','id');\n");
      sb.append("call set_table_property('")
          .append(tableName)
          .append("','binlog.level','replica');\n");
      sb.append("call set_table_property('")
          .append(tableName)
          .append("','binlog.ttl','3600');\n");
    }
    if (!enableBitmap) {
      sb.append("call set_table_property('")
          .append(tableName)
          .append("','bitmap_columns','');\n");
    }
    sb.append("end;");
    String sql = sb.toString();
    LOG.info("sql:{}", sql);
    try (Statement stat = conn.createStatement()) {
      stat.execute(sb.toString());
    }
  }

  public static void vaccumTable(Connection conn, String tableName) throws SQLException {
      String sql = "vacuum " + tableName;
      try (Statement stat = conn.createStatement()) {
        stat.execute(sql);
      }
      LOG.info("sql:{}", sql);
  }

  public static void disableAffectedRows(Connection conn) throws SQLException {
    String sql = "set hg_experimental_enable_fixed_dispatcher_affected_rows = off";
    try (Statement stat = conn. createStatement()) {
      stat.execute(sql);
    }
    LOG.info("sql:{}", sql);
  }

  public static int getNumberFrontends(Connection connection) {
    try (Statement statement = connection.createStatement()) {
      int maxConnections = 128;
      try (ResultSet rs = statement.executeQuery("show max_connections;")) {
        if (rs.next()) {
          maxConnections = rs.getInt(1);
        }
      }
      LOG.info("get max_connections for single fe node.");
      int instanceMaxConnections = 0;
      try (ResultSet rs = statement.executeQuery("select instance_max_connections();")) {
        if (rs.next()) {
          instanceMaxConnections = rs.getInt(1);
        }
      }
      LOG.info("get max_connections for instance.");
      return instanceMaxConnections / maxConnections;
    } catch (SQLException e) {
      // function instance_max_connections is only supported for hologres version > 1.3.20.
      if (e.getMessage().contains("function instance_max_connections() does not exist")) {
        LOG.warn("Failed to get hologres frontends number.", e);
        return 0;
      }
      throw new RuntimeException("Failed to get hologres frontends number.", e);
    }
  }

  public static void dropTableByHoloClient(HoloConfig config, String tableName) throws Exception {
    try (HoloClient client = new HoloClient(config)) {
      client.sql(conn -> {
        try (Statement stat = conn.createStatement()) {
          stat.execute("drop table if exists " + tableName);
        }
        return null;
      }).get();
    }
  }
}
