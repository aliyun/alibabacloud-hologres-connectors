package com.alibaba.hologres.performace.client;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SqlUtil {
  public static Logger LOG = LoggerFactory.getLogger(SqlUtil.class);

  public static void createTable(Connection conn, boolean partition, String tableName,
      int columnCount, int shardCount, String orientation, boolean enableBinlog, int binlogTTL,
      boolean enableBitmap, boolean additionTsColumn, boolean hasPk, boolean prefixPk , String dataColumnType) throws
      SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("set hg_experimental_force_sync_replay = on;\n");
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
          .append("','binlog.ttl','")
          .append(binlogTTL)
          .append("');\n");
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

  public static void setOnSessionThreadSize(Connection conn, int threadSize) throws SQLException {
    String sql = "set hg_experimental_fixed_thread_pool_size = " + threadSize;
    try (Statement stat = conn.createStatement()) {
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

  public static void createBinLogExtension(Connection conn) throws SQLException {
    String sql = "create extension if not exists hg_binlog";
    try (Statement stat = conn.createStatement()) {
      stat.execute(sql);
    }
    LOG.info("sql:{}", sql);
  }

  public static void createPublication(Connection conn, String publicationName, String tableName) throws SQLException {
    String sql = "create publication " + publicationName + " for table " + tableName;
    try (Statement stat = conn.createStatement()) {
      stat.execute(sql);
    }
    LOG.info("sql:{}", sql);
  }

  public static void createSlot(Connection conn, String publicationName, String slotName) throws SQLException {
    String sql = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "')";
    try(Statement stat = conn.createStatement()) {
      stat.execute(sql);
    }
    LOG.info("sql:{}", sql);
  }

  public static void dropPublication(Connection conn, String publicationName) throws SQLException {
    String sql = "drop publication if exists " + publicationName;
    try (Statement stat = conn.createStatement()) {
      stat.execute(sql);
    }
    LOG.info("sql:{}", sql);
  }

  public static void dropSlot(Connection conn, String slotName) throws SQLException {
    String sql = "call hg_drop_logical_replication_slot('" + slotName + "')";
    try(Statement stat = conn.createStatement()) {
      stat.execute(sql);
    }
    LOG.info("sql:{}", sql);
  }

  public static void deleteReplicationProgress(Connection conn, String slotName) throws SQLException {
    String sql = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "'";
    try(Statement stat = conn.createStatement()) {
      stat.execute(sql);
    }
    LOG.info("sql:{}", sql);
  }

  public static void createUserIfNotExists(Connection conn, String userName) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("set hg_experimental_force_sync_replay = on; ")
        .append("create user \"")
        .append(userName)
        .append("\";");
    String sql = sb.toString();
    LOG.info("sql:{}", sql);
    try (Statement stat = conn.createStatement()) {
      stat.execute(sql);
    } catch (SQLException e) {
      if (!e.getMessage().contains("already exists")) {
        throw e;
      }
    }
  }

  public static void checkCurrentUser(Connection conn, String userName) throws SQLException {
    String sql = "select current_user;";
    LOG.debug("sql:{}", sql);
    try (Statement stat = conn.createStatement()) {
      ResultSet resultSet = stat.executeQuery(sql);
      if (!resultSet.next()) {
        throw new RuntimeException("No result");
      }

      String result = resultSet.getString("current_user");
      if (!result.equals(userName)) {
        throw new RuntimeException("Current user is not as expected");
      }
    }
  }
}
