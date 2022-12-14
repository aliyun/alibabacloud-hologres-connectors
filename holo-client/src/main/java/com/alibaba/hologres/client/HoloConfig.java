/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.WriteFailStrategy;
import com.alibaba.hologres.client.model.WriteMode;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * config class for holo-client.
 */
public class HoloConfig implements Serializable {

	public static final int DEFAULT_BATCH_SIZE = 512;
	public static final long DEFAULT_BATCH_BYTE_SIZE = 2L * 1024L * 1024L;
	public static final WriteMode DEFAULT_WRITE_MODE = WriteMode.INSERT_OR_REPLACE;

	public static final int DEFAULT_READ_BATCH_SIZE = 128;
	public static final int DEFAULT_READ_QUEUE = 256;

	//----------------------------write conf--------------------------------------------
	/**
	 * 在AsyncCommit为true，调用put方法时，当记录数>=writeBatchSize 或 总记录字节数数>=writeBatchByteSize.
	 * 调用flush进行批量提交.
	 * 默认为512
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int writeBatchSize = DEFAULT_BATCH_SIZE;

	/**
	 * 在AsyncCommit为true，调用put方法时，当记录数>=writeBatchSize 或 总记录字节数数>=writeBatchByteSize.
	 * 调用flush进行List批量提交.
	 * 默认为2MB
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long writeBatchByteSize = DEFAULT_BATCH_BYTE_SIZE;

	/**
	 * 所有表攒批总共的最大batchSize
	 * 调用flush进行List批量提交.
	 * 默认为20MB
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long writeBatchTotalByteSize = DEFAULT_BATCH_BYTE_SIZE * 10;

	/**
	 * 当INSERT目标表为有主键的表时采用不同策略.
	 * INSERT_OR_IGNORE 当主键冲突时，不写入
	 * INSERT_OR_UPDATE 当主键冲突时，更新相应列
	 * INSERT_OR_REPLACE当主键冲突时，更新所有列
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	WriteMode writeMode = DEFAULT_WRITE_MODE;

	/**
	 * 启用后，当put分区表时，若分区不存在将自动创建，默认false.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean dynamicPartition = false;

	/**
	 * 在AsyncCommit为true，当记录数>=writeBatchSize 或 总记录字节数数>=writeBatchByteSize 或距离上次flush超过writeMaxIntervalMs毫秒 调用flush进行提交     * 默认为100MB.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long writeMaxIntervalMs = 10000L;

	/**
	 * 当INSERT失败采取的策略.
	 * TRY_ONE_BY_NE
	 * NONE
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	WriteFailStrategy writeFailStrategy = WriteFailStrategy.TRY_ONE_BY_ONE;

	/**
	 * put操作的并发数.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int writeThreadSize = 1;

	/**
	 * 当将Number写入Date/timestamp/timestamptz列时，将number视作距离1970-01-01 00:00:00 +00:00的毫秒数.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean inputNumberAsEpochMsForDatetimeColumn = false;

	/**
	 * 当将Number写入Date/timestamp/timestamptz列时，将number视作距离1970-01-01 00:00:00 +00:00的毫秒数.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean inputStringAsEpochMsForDatetimeColumn = false;

	/**
	 * 启用时，not null且未在表上设置default的字段传入null时，将转为默认值.
	 * String
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean enableDefaultForNotNullColumn = true;

	/**
	 * defaultTimestamp.
	 * String
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	String defaultTimestampText = null;

	/**
	 * 写入shard数重计算的间隔.
	 * long
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long writerShardCountResizeIntervalMs = 30000L;

	/**
	 * 开启将text列value中的\u0000替换为"".
	 * boolean
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean removeU0000InTextColumnValue = true;

	/**
	 * 全局flush的时间间隔.
	 * boolean
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long forceFlushInterval = -1L;

	/**
	 * 最大shard数.
	 * boolean
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int maxShardCount = -1;

	/**
	 * CopyIn时，InputStream和网络OutputStream之间的交互数据的buffer大小.
	 * int
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int copyInBufferSize = 65536;

	/**
	 * 多久打印一次写入数据采样.
	 * int
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long recordSampleInterval = -1L;

	/**
	 * 强制使用老方式做put请求 insert into xxx values (),() on conflict.
	 * 不强制的话，对于高版本holo会自动采用insert into xxx select unnest() on conflict的方式写入
	 * boolean
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean useLegacyPutHandler = false;

	/**
	 * 写入时一条sql的最大行数，仅unnest模式生效.
	 * [TODO] 仍在测试中.
	 * int
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int maxRowsPerSql = Integer.MAX_VALUE;

	/**
	 * 写入时一条sql的最大字节数，仅unnest模式生效.
	 * [TODO] 仍在测试中.
	 * long
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long maxBytesPerSql = Long.MAX_VALUE;
	//--------------------------read conf-------------------------------------------------
	/**
	 * 最多一次将readBatchSize条Get请求合并提交，默认128.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int readBatchSize = DEFAULT_READ_BATCH_SIZE;
	/**
	 * get请求缓冲池大小，默认256.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int readBatchQueueSize = DEFAULT_READ_QUEUE;

	/**
	 * get操作的并发数.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int readThreadSize = 1;

	/**
	 * get操作的超时时间.
	 * 默认值0表示不超时，会在两处位置生效：
	 * 		1.get操作从用户开始执行到client准备提交到holo的等待时间，如果这个阶段超时不会重试直接抛出异常.
	 * 		2.get sql的执行超时，即statement query timeout，最小1s.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int readTimeoutMilliseconds = 0;

	/**
	 * get请求重试次数, 与默认的retryCount分别设置，默认1不会retry.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int readRetryCount = 1;

	//--------------------------scan conf-------------------------------------------------
	/**
	 * scan每次fetch的大小.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int scanFetchSize = 2000;
	/**
	 * scan的超时时间.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int scanTimeoutSeconds = 60;

	//--------------------------binlog read conf-------------------------------------------------
	/**
	 * 一次读取 binlogReadBatchSize 条 Binlog 数据.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int binlogReadBatchSize = 1024;

	/**
	 * binlogRead 发送BinlogHeartBeatRecord的间隔.
	 * -1表示不发送
	 * 当binlog没有新数据，每binlogHeartBeatIntervalMs会下发一条BinlogHeartBeatRecord，record的timestamp表示截止到这个时间的数据都已经消费完了.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long binlogHeartBeatIntervalMs = -1L;

	/**
	 * 是否需要忽略Delete类型的Binlog.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean binlogIgnoreDelete = false;

	/**
	 * 是否需要忽略BeforeUpdate类型的Binlog.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean binlogIgnoreBeforeUpdate = false;

	//---------------------------conn conf------------------------------------------
	/**
	 * 请求重试次数，默认3.
	 * 这个名字应该叫做maxTryCount，而不是retryCount，设为1其实是不会retry的
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int retryCount = 3;

	/**
	 * 每次重试等待时间为  当前重试次数*retrySleepMs + retrySleepInitMs.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long retrySleepStepMs = 10000L;

	/**
	 * 每次重试等待时间为  当前重试次数*retrySleepMs + retrySleepInitMs.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long retrySleepInitMs = 1000L;

	/**
	 * 每个get和put的后台连接在空闲超过connectionMaxIdleMs后将被释放(再次使用时会自动重新连接).
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long connectionMaxIdleMs = 60000L;

	/**
	 * meta信息缓存时间(ms).
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	long metaCacheTTL = 60000L;

	/**
	 * meta缓存剩余时间低于 metaCacheTTL/metaAutoRefreshFactor 将被自动刷新.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int metaAutoRefreshFactor = 4;

	/**
	 * 执行hg_internal_refresh_meta的默认超时时间(单位为秒).
	 * 若<=0则不执行
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int refreshMetaTimeout = 10;

	/**
	 * connection建立后是否执行hg_internal_refresh_meta.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean refreshMetaAfterConnectionCreated = true;

	/**
	 * 获取tableSchema前是否执行hg_internal_refresh_meta.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean refreshMetaBeforeGetTableSchema = true;

	/**
	 * 是否直连fe.
	 * 当弹内用户的vip带宽不能满足需求，需要通过绕过vip直接连接fe时可打开该开关
	 * 如果是公有云或其他与部署holo机器网络不通环境上的用户请勿打开，否则无法连接
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean enableDirectConnection = false;

	//------------------------endpoint conf--------------------------------------------
	/**
	 * 顾名思义，jdbcUrl.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	String jdbcUrl;

	/**
	 * jdbcUrl，必填.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	String username;
	/**
	 * jdbcUrl，必填.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	String password;

	/**
	 * 是否使用fixed fe模式进行数据写入和点查.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean useFixedFe = false;

	/**
	 * 使用fixed fe进行数据写入和点查时， 其他action使用的连接数.
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	int connectionSizeWhenUseFixedFe = 1;

	String appName = "holo-client";

	boolean enableShutdownHook = false;

	public WriteMode getWriteMode() {
		return writeMode;
	}

	public void setWriteMode(WriteMode writeMode) {
		this.writeMode = writeMode;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isUseFixedFe() {
		return useFixedFe;
	}

	public void setUseFixedFe(boolean useFixedFe) {
		this.useFixedFe = useFixedFe;
	}

	public int getConnectionSizeWhenUseFixedFe() {
		return connectionSizeWhenUseFixedFe;
	}

	public void setConnectionSizeWhenUseFixedFe(int connectionSizeWhenUseFixedFe) {
		this.connectionSizeWhenUseFixedFe = connectionSizeWhenUseFixedFe;
	}

	public int getWriteBatchSize() {
		return writeBatchSize;
	}

	public void setWriteBatchSize(int batchSize) {
		this.writeBatchSize = batchSize;
	}

	public long getWriteBatchByteSize() {
		return writeBatchByteSize;
	}

	public void setWriteBatchByteSize(long batchByteSize) {
		this.writeBatchByteSize = batchByteSize;
	}

	public int getReadBatchSize() {
		return readBatchSize;
	}

	public void setReadBatchSize(int readBatchSize) {
		this.readBatchSize = readBatchSize;
	}

	public int getReadBatchQueueSize() {
		return readBatchQueueSize;
	}

	public void setReadBatchQueueSize(int readBatchQueueSize) {
		this.readBatchQueueSize = readBatchQueueSize;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

	public long getRetrySleepStepMs() {
		return retrySleepStepMs;
	}

	public void setRetrySleepStepMs(long retrySleepStepMs) {
		this.retrySleepStepMs = retrySleepStepMs;
	}

	public long getRetrySleepInitMs() {
		return retrySleepInitMs;
	}

	public void setRetrySleepInitMs(long retrySleepInitMs) {
		this.retrySleepInitMs = retrySleepInitMs;
	}

	public boolean isDynamicPartition() {
		return dynamicPartition;
	}

	public void setDynamicPartition(boolean dynamicPartition) {
		this.dynamicPartition = dynamicPartition;
	}

	public long getWriteMaxIntervalMs() {
		return writeMaxIntervalMs;
	}

	public void setWriteMaxIntervalMs(long writeMaxIntervalMs) {
		this.writeMaxIntervalMs = writeMaxIntervalMs;
	}

	public WriteFailStrategy getWriteFailStrategy() {
		return writeFailStrategy;
	}

	public void setWriteFailStrategy(WriteFailStrategy writeFailStrategy) {
		this.writeFailStrategy = writeFailStrategy;
	}

	public int getReadThreadSize() {
		return readThreadSize;
	}

	public void setReadThreadSize(int readThreadSize) {
		this.readThreadSize = readThreadSize;
	}

	public int getReadTimeoutMilliseconds() {
		return readTimeoutMilliseconds;
	}

	public void setReadTimeoutMilliseconds(int readTimeoutSeconds) {
		this.readTimeoutMilliseconds = readTimeoutSeconds;
	}

	public int getReadRetryCount() {
		return readRetryCount;
	}

	public void setReadRetryCount(int readRetryCount) {
		this.readRetryCount = readRetryCount;
	}

	public long getConnectionMaxIdleMs() {
		return connectionMaxIdleMs;
	}

	public void setConnectionMaxIdleMs(long connectionMaxIdleMs) {
		this.connectionMaxIdleMs = connectionMaxIdleMs;
	}

	public int getWriteThreadSize() {
		return writeThreadSize;
	}

	public void setWriteThreadSize(int writeThreadSize) {
		this.writeThreadSize = writeThreadSize;
	}

	public boolean isInputNumberAsEpochMsForDatetimeColumn() {
		return inputNumberAsEpochMsForDatetimeColumn;
	}

	public void setInputNumberAsEpochMsForDatetimeColumn(boolean inputNumberAsEpochMsForDatetimeColumn) {
		this.inputNumberAsEpochMsForDatetimeColumn = inputNumberAsEpochMsForDatetimeColumn;
	}

	public long getMetaCacheTTL() {
		return metaCacheTTL;
	}

	public void setMetaCacheTTL(long metaCacheTTL) {
		this.metaCacheTTL = metaCacheTTL;
	}

	public boolean isInputStringAsEpochMsForDatetimeColumn() {
		return inputStringAsEpochMsForDatetimeColumn;
	}

	public void setInputStringAsEpochMsForDatetimeColumn(boolean inputStringAsEpochMsForDatetimeColumn) {
		this.inputStringAsEpochMsForDatetimeColumn = inputStringAsEpochMsForDatetimeColumn;
	}

	public boolean isEnableDefaultForNotNullColumn() {
		return enableDefaultForNotNullColumn;
	}

	public void setEnableDefaultForNotNullColumn(boolean enableDefaultForNotNullColumn) {
		this.enableDefaultForNotNullColumn = enableDefaultForNotNullColumn;
	}

	public String getDefaultTimestampText() {
		return defaultTimestampText;
	}

	public void setDefaultTimestampText(String defaultTimestampText) {
		this.defaultTimestampText = defaultTimestampText;
	}

	public long getWriteBatchTotalByteSize() {
		return writeBatchTotalByteSize;
	}

	public void setWriteBatchTotalByteSize(long writeBatchTotalByteSize) {
		this.writeBatchTotalByteSize = writeBatchTotalByteSize;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public int getScanFetchSize() {
		return scanFetchSize;
	}

	public int getScanTimeoutSeconds() {
		return scanTimeoutSeconds;
	}

	public void setScanFetchSize(int scanFetchSize) {
		this.scanFetchSize = scanFetchSize;
	}

	public void setScanTimeoutSeconds(int scanTimeoutSeconds) {
		this.scanTimeoutSeconds = scanTimeoutSeconds;
	}

	public int getMetaAutoRefreshFactor() {
		return metaAutoRefreshFactor;
	}

	public void setMetaAutoRefreshFactor(int metaAutoRefreshFactor) {
		this.metaAutoRefreshFactor = metaAutoRefreshFactor;
	}

	public long getWriterShardCountResizeIntervalMs() {
		return writerShardCountResizeIntervalMs;
	}

	public void setWriterShardCountResizeIntervalMs(long writerShardCountResizeIntervalMs) {
		this.writerShardCountResizeIntervalMs = writerShardCountResizeIntervalMs;
	}

	public boolean isRemoveU0000InTextColumnValue() {
		return removeU0000InTextColumnValue;
	}

	public void setRemoveU0000InTextColumnValue(boolean removeU0000InTextColumnValue) {
		this.removeU0000InTextColumnValue = removeU0000InTextColumnValue;
	}

	public int getRefreshMetaTimeout() {
		return refreshMetaTimeout;
	}

	public void setRefreshMetaTimeout(int refreshMetaTimeout) {
		this.refreshMetaTimeout = refreshMetaTimeout;
	}

	public boolean isRefreshMetaAfterConnectionCreated() {
		return refreshMetaAfterConnectionCreated;
	}

	public void setRefreshMetaAfterConnectionCreated(boolean refreshMetaAfterConnectionCreated) {
		this.refreshMetaAfterConnectionCreated = refreshMetaAfterConnectionCreated;
	}

	public boolean isRefreshMetaBeforeGetTableSchema() {
		return refreshMetaBeforeGetTableSchema;
	}

	public void setRefreshMetaBeforeGetTableSchema(boolean refreshMetaBeforeGetTableSchema) {
		this.refreshMetaBeforeGetTableSchema = refreshMetaBeforeGetTableSchema;
	}

	public boolean isEnableDirectConnection() {
		return enableDirectConnection;
	}

	public void setEnableDirectConnection(boolean enableDirectConnection) {
		this.enableDirectConnection = enableDirectConnection;
	}

	public long getForceFlushInterval() {
		return forceFlushInterval;
	}

	public void setForceFlushInterval(long forceFlushInterval) {
		this.forceFlushInterval = forceFlushInterval;
	}

	public int getMaxShardCount() {
		return maxShardCount;
	}

	public void setMaxShardCount(int maxShardCount) {
		this.maxShardCount = maxShardCount;
	}

	public int getCopyInBufferSize() {
		return copyInBufferSize;
	}

	public void setCopyInBufferSize(int copyInBufferSize) {
		this.copyInBufferSize = copyInBufferSize;
	}

	public long getRecordSampleInterval() {
		return recordSampleInterval;
	}

	public void setRecordSampleInterval(long recordSampleInterval) {
		this.recordSampleInterval = recordSampleInterval;
	}

	public int getBinlogReadBatchSize() {
		return binlogReadBatchSize;
	}

	public void setBinlogReadBatchSize(int binlogReadBatchSize) {
		this.binlogReadBatchSize = binlogReadBatchSize;
	}

	public long getBinlogHeartBeatIntervalMs() {
		return binlogHeartBeatIntervalMs;
	}

	public void setBinlogHeartBeatIntervalMs(long binlogHeartBeatIntervalMs) {
		this.binlogHeartBeatIntervalMs = binlogHeartBeatIntervalMs;
	}

	public boolean getBinlogIgnoreDelete() {
		return binlogIgnoreDelete;
	}

	public void setBinlogIgnoreDelete(boolean binlogIgnoreDelete) {
		this.binlogIgnoreDelete = binlogIgnoreDelete;
	}

	public boolean getBinlogIgnoreBeforeUpdate() {
		return binlogIgnoreBeforeUpdate;
	}

	public void setBinlogIgnoreBeforeUpdate(boolean binlogIgnoreBeforeUpdate) {
		this.binlogIgnoreBeforeUpdate = binlogIgnoreBeforeUpdate;
	}

	public boolean isEnableShutdownHook() {
		return enableShutdownHook;
	}

	public void setEnableShutdownHook(boolean enableShutdownHook) {
		this.enableShutdownHook = enableShutdownHook;
	}

	public boolean isUseLegacyPutHandler() {
		return useLegacyPutHandler;
	}

	public void setUseLegacyPutHandler(boolean useLegacyPutHandler) {
		this.useLegacyPutHandler = useLegacyPutHandler;
	}

	public int getMaxRowsPerSql() {
		return maxRowsPerSql;
	}

	public void setMaxRowsPerSql(int maxRowsPerSql) {
		this.maxRowsPerSql = maxRowsPerSql;
	}

	public long getMaxBytesPerSql() {
		return maxBytesPerSql;
	}

	public void setMaxBytesPerSql(long maxBytesPerSql) {
		this.maxBytesPerSql = maxBytesPerSql;
	}

	public static String[] getPropertyKeys() {
		Field[] fields = HoloConfig.class.getDeclaredFields();
		String[] propertyKeys = new String[fields.length];
		int index = 0;
		for (Field field : fields) {
			propertyKeys[index++] = field.getName();
		}
		return propertyKeys;
	}
}
