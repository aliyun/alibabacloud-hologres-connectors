/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.function.FunctionWithSQLException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.impl.action.CopyAction;
import com.alibaba.hologres.client.impl.action.PutAction;
import com.alibaba.hologres.client.impl.action.ScanAction;
import com.alibaba.hologres.client.impl.action.SqlAction;
import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.impl.binlog.BinlogRecordCollector;
import com.alibaba.hologres.client.impl.binlog.Committer;
import com.alibaba.hologres.client.impl.binlog.TableSchemaSupplier;
import com.alibaba.hologres.client.impl.binlog.action.BinlogAction;
import com.alibaba.hologres.client.impl.collector.ActionCollector;
import com.alibaba.hologres.client.impl.collector.BatchState;
import com.alibaba.hologres.client.impl.copy.CopyContext;
import com.alibaba.hologres.client.impl.copy.InternalPipedOutputStream;
import com.alibaba.hologres.client.model.ExportContext;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.ImportContext;
import com.alibaba.hologres.client.model.Partition;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.model.binlog.BinlogPartitionSubscribeMode;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutRecord;
import com.alibaba.hologres.client.model.checkandput.CheckCompareOp;
import com.alibaba.hologres.client.utils.PartitionUtil;
import com.alibaba.hologres.client.utils.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 线程不安全，每个client，最多会创建2个JDBC connection，读写分离.
 */
public class HoloClient implements Closeable {

	public static final Logger LOGGER = LoggerFactory.getLogger(HoloClient.class);

	static {
		LOGGER.info("=========holo-client version==========");
		LOGGER.info("version:{}", com.alibaba.hologres.client.Version.version);
		LOGGER.info("revision:{}", com.alibaba.hologres.client.Version.revision);
		LOGGER.info("date:{}", com.alibaba.hologres.client.Version.date);
		LOGGER.info("======================================");
	}

	static {
		// Load DriverManager first to avoid deadlock between DriverManager's
		// static initialization block and specific driver class's static
		// initialization block when two different driver classes are loading
		// concurrently using Class.forName while DriverManager is uninitialized
		// before.
		//
		// This could happen in JDK 8 but not above as driver loading has been
		// moved out of DriverManager's static initialization block since JDK 9.
		DriverManager.getDrivers();
	}

	private ActionCollector collector;

	/**
	 * 是否使用fixed fe.
	 * 开启的话会创建fixed pool，用于执行点查、写入以及prefix scan.
	 */
	private final boolean useFixedFe;
	private ExecutionPool fixedPool = null;

	private ExecutionPool pool = null;
	private final HoloConfig config;

	/**
	 * 在AsyncCommit为true，调用put方法时，当记录数>=writeBatchSize 或 总记录字节数数>=writeBatchByteSize 调用flush进行提交.
	 * 否则每次调用put都会调用flush.
	 * 默认为true
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean asyncCommit = true;

	/**
	 * 是否shading打包后的环境.
	 * 打包后的运行环境，isShadingEnv=true，将把jdbc:postgresql:改为jdbc:hologres:
	 * 本地跑测试时，isShadingEnv=false。此时pgjdbc没有shading，仍然要使用jdbc:postgresql:
	 */
	boolean isShadingEnv = false;

	boolean isEmbeddedPool = false;
	boolean isEmbeddedFixedPool = false;

	public HoloClient(HoloConfig config) throws HoloClientException {
		try {
			DriverManager.getDrivers();
			Class.forName("com.alibaba.hologres.org.postgresql.Driver");
			isShadingEnv = true;
		} catch (Exception e) {
			try {
				DriverManager.getDrivers();
				Class.forName("org.postgresql.Driver");
			} catch (Exception e2) {
				throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "load driver fail", e);
			}
		}
		checkConfig(config);
		this.config = config;
		this.useFixedFe = config.isUseFixedFe();
	}

	private void checkConfig(HoloConfig config) throws HoloClientException {
		if (config.getJdbcUrl() == null || config.getJdbcUrl().isEmpty()) {
			throw new HoloClientException(ExceptionCode.INVALID_Config, "jdbcUrl cannot be null");
		}
		if (config.getPassword() == null || config.getPassword().isEmpty()) {
			throw new HoloClientException(ExceptionCode.INVALID_Config, "password cannot be null");
		}

		if (config.getUsername() == null || config.getUsername().isEmpty()) {
			throw new HoloClientException(ExceptionCode.INVALID_Config, "username cannot be null");
		}
		if (config.getWriteBatchSize() < 1) {
			throw new HoloClientException(ExceptionCode.INVALID_Config, "batchSize must > 0");
		}
		if (config.getWriteBatchByteSize() < 1) {
			throw new HoloClientException(ExceptionCode.INVALID_Config, "batchByteSize must > 0");
		}
	}

	public TableSchema getTableSchema(String tableName) throws HoloClientException {
		return getTableSchema(TableName.valueOf(tableName), false);
	}

	public TableSchema getTableSchema(String tableName, boolean noCache) throws HoloClientException {
		return getTableSchema(TableName.valueOf(tableName), noCache);
	}

	public TableSchema getTableSchema(TableName tableName) throws HoloClientException {
		return getTableSchema(tableName, false);
	}

	public TableSchema getTableSchema(TableName tableName, boolean noCache) throws HoloClientException {
		ensurePoolOpen();
		return pool.getOrSubmitTableSchema(tableName, noCache);
	}

	private void checkGet(Get get) throws HoloClientException {
		if (get == null) {
			throw new HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, "Get cannot be null");
		}
		if (get.getRecord().getSchema().getPrimaryKeys().length == 0) {
			throw new HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, "Get table must have primary key:" + get.getRecord().getSchema().getTableNameObj().getFullName());
		}

		for (int index : get.getRecord().getKeyIndex()) {
			if (!get.getRecord().isSet(index) || null == get.getRecord().getObject(index)) {
				throw new HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, "Get primary key cannot be null:" + get.getRecord().getSchema().getColumnSchema()[index].getName());
			}
		}
	}

	private void checkPut(Put put) throws HoloClientException {
		if (put == null) {
			throw new HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, "Put cannot be null");
		}
		for (int index : put.getRecord().getKeyIndex()) {
			if ((!put.getRecord().isSet(index) || null == put.getRecord().getObject(index)) && put.getRecord().getSchema().getColumn(index).getDefaultValue() == null) {
				throw new HoloClientWithDetailsException(ExceptionCode.CONSTRAINT_VIOLATION, "Put primary key cannot be null:" + put.getRecord().getSchema().getColumnSchema()[index].getName(), put.getRecord());
			}
		}
		if (put.getRecord().getSchema().isPartitionParentTable() && (!put.getRecord().isSet(put.getRecord().getSchema().getPartitionIndex()) || null == put.getRecord().getObject(put.getRecord().getSchema().getPartitionIndex()))) {
			throw new HoloClientWithDetailsException(ExceptionCode.CONSTRAINT_VIOLATION, "Put partition key cannot be null:" + put.getRecord().getSchema().getColumnSchema()[put.getRecord().getSchema().getPartitionIndex()].getName(), put.getRecord());

		}
		if (put.getRecord().getType() == Put.MutationType.DELETE && put.getRecord().getSchema().getPrimaryKeys().length == 0) {
			throw new HoloClientWithDetailsException(ExceptionCode.CONSTRAINT_VIOLATION, "Delete Put table must have primary key:" + put.getRecord().getSchema().getTableNameObj().getFullName(), put.getRecord());
		}
	}

	/** 检查CheckAndPut内容并进行重写. */
	private void checkCheckAndPut(CheckAndPut put) throws HoloClientException {
		if (put == null) {
			throw new HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, "CheckAndPut cannot be null");
		}
		if (put.getRecord().getSchema().getPrimaryKeys().length == 0) {
			throw new HoloClientWithDetailsException(ExceptionCode.CONSTRAINT_VIOLATION, "CheckAndPut need table have primary key:" + put.getRecord().getSchema().getTableNameObj().getFullName(), put.getRecord());
		}
		for (int index : put.getRecord().getKeyIndex()) {
			if ((!put.getRecord().isSet(index) || null == put.getRecord().getObject(index)) && put.getRecord().getSchema().getColumn(index).getDefaultValue() == null) {
				throw new HoloClientWithDetailsException(ExceptionCode.CONSTRAINT_VIOLATION, "CheckAndPut primary key cannot be null:" + put.getRecord().getSchema().getColumnSchema()[index].getName(), put.getRecord());
			}
		}
		if (put.getRecord().getSchema().isPartitionParentTable() && (!put.getRecord().isSet(put.getRecord().getSchema().getPartitionIndex()) || null == put.getRecord().getObject(put.getRecord().getSchema().getPartitionIndex()))) {
			throw new HoloClientWithDetailsException(ExceptionCode.CONSTRAINT_VIOLATION, "CheckAndPut partition key cannot be null:" + put.getRecord().getSchema().getColumnSchema()[put.getRecord().getSchema().getPartitionIndex()].getName(), put.getRecord());
		}
		if (config.getWriteMode() == WriteMode.INSERT_OR_IGNORE) {
			throw new HoloClientException(ExceptionCode.NOT_SUPPORTED, "CheckAndPut not supports writeMode insertOrIgnore.");
		}
		CheckAndPutRecord record = put.getRecord();
		String checkColumnName = record.getCheckAndPutCondition().getCheckColumnName();
		CheckCompareOp checkOp = record.getCheckAndPutCondition().getCheckOp();
		Object checkValue = record.getCheckAndPutCondition().getCheckValue();
		Object nullValue = record.getCheckAndPutCondition().getNullValue();
		Integer checkColumnIndex = record.getSchema().getColumnIndex(checkColumnName);
		if (checkColumnIndex == null || checkColumnIndex < 0) {
			throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "checkColumn " + checkColumnName + " is not exists in table " + put.getRecord().getSchema().getTableNameObj().getFullName(), put.getRecord());
		} else {
			// CheckAndPutCondition 可能是通过columnName初始化的
			put.getRecord().getCheckAndPutCondition().setCheckColumn(record.getSchema().getColumn(checkColumnIndex));
		}

		if (checkOp == CheckCompareOp.IS_NULL || checkOp == CheckCompareOp.IS_NOT_NULL) {
			// is null,is not null不需要做coalesce,不关心nullValue,checkValue的值
			nullValue = null;
			checkValue = null;
			put.getRecord().getCheckAndPutCondition().setNullValue(nullValue);
			put.getRecord().getCheckAndPutCondition().setCheckValue(checkValue);
		} else {
			// >,>=,=,<>,<,<=操作符不能和null进行比较（结果恒为false），需要提供nullValue进行重写，相当于sql中的coalesce函数
			if (nullValue == null && record.getCheckAndPutCondition().getCheckColumn().getAllowNull()) {
				LOGGER.warn("When a field allows null, it is recommended to set nullValue to prevent null fields from being updated.");
			}
			// checkValue没有设置，表示要使用当前put中的checkColumn值和已有的值进行比较，所以Record的checkColumn必须被set
			if (checkValue == null && !put.getRecord().isSet(checkColumnIndex)) {
				throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "checkColumn " + checkColumnName + " should be set when not set checkValue.", put.getRecord());
			}
			// checkValue没有设置，表示要使用当前put中的checkColumn值和已有的值进行比较，所以Record的checkColumn必须被set，且对于delete来说不能设置为null(抛出异常: internal error: binaryrow should not be empty)
			if (checkValue == null && put.getRecord().getObject(checkColumnIndex) == null && put.getRecord().getType() == Put.MutationType.DELETE) {
				// delete from table where (pk = $1 and $2 > checkColumn). 这里的$2不能是null
				throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "checkColumn " + checkColumnName + " should be set not null when not set checkValue and mutationType is delete.", put.getRecord());
			}
		}
		// delete不支持攒批，直接将delete的checkValue设置为put中的checkColumn字段的值, 这样不同的Condition会分批提交
		if (checkValue == null && put.getRecord().getType() == Put.MutationType.DELETE && checkOp != CheckCompareOp.IS_NULL && checkOp != CheckCompareOp.IS_NOT_NULL) {
			checkValue = put.getRecord().getObject(checkColumnIndex);
			put.getRecord().getCheckAndPutCondition().setCheckValue(checkValue);
		}
	}

	public CompletableFuture<Record> get(Get get) throws HoloClientException {
		ensurePoolOpen();
		checkGet(get);
		get.setStartTime(System.nanoTime());
		get.setFuture(new CompletableFuture<>());
		if (get.isFullColumn()) {
			for (int i = 0; i < get.getRecord().getSchema().getColumnSchema().length; ++i) {
				if (!get.getRecord().isSet(i)) {
					get.getRecord().setObject(i, null);
				}
			}
		}
		try {
			if (rewriteForPartitionTable(get.getRecord(), false, false)) {
				get.getFuture().complete(null);
			}
		} catch (HoloClientException e) {
			get.getFuture().completeExceptionally(e);
		}
		collector.appendGet(get);
		return get.getFuture();
	}

	public List<CompletableFuture<Record>> get(List<Get> gets) throws HoloClientException {
		ensurePoolOpen();
		for (Get get : gets) {
			checkGet(get);
			get.setStartTime(System.nanoTime());
			get.setFuture(new CompletableFuture<>());
			if (get.isFullColumn()) {
				for (int i = 0; i < get.getRecord().getSchema().getColumnSchema().length; ++i) {
					if (!get.getRecord().isSet(i)) {
						get.getRecord().setObject(i, null);
					}
				}
			}
			try {
				if (rewriteForPartitionTable(get.getRecord(), false, false)) {
					get.getFuture().complete(null);
				}
			} catch (HoloClientException e) {
				get.getFuture().completeExceptionally(e);
			}
		}
		List<CompletableFuture<Record>> ret = new ArrayList<>();
		collector.appendGet(gets);
		for (Get get : gets) {
			ret.add(get.getFuture());
		}
		return ret;
	}

	public <T> CompletableFuture<T> sql(FunctionWithSQLException<Connection, T> func) throws HoloClientException {
		ensurePoolOpen();
		SqlAction<T> action = new SqlAction<>(func);
		while (!pool.submit(action)) {

		}
		return action.getFuture();
	}

	public RecordScanner scan(Scan scan) throws HoloClientException {
		return doScan(scan).getResult();
	}

	public CompletableFuture<RecordScanner> asyncScan(Scan scan) throws HoloClientException {
		return doScan(scan).getFuture();
	}

	private ScanAction doScan(Scan scan) throws HoloClientException {
		ensurePoolOpen();
		ScanAction action = new ScanAction(scan);
		ExecutionPool execPool = getExecPool();
		while (!execPool.submit(action)) {

		}
		return action;
	}

	private void ensurePoolOpen() throws HoloClientException {
		if (pool == null) {
			synchronized (this) {
				if (pool == null) {
					ExecutionPool temp = new ExecutionPool("embedded-" + config.getAppName(), config, isShadingEnv, false);
					// 当useFixedFe为true 则不赋值新的collector，调用这个函数仅仅为了把pool标记为started.
					ActionCollector tempCollector = temp.register(this, config);
					if (!this.useFixedFe) {
						collector = tempCollector;
					}
					pool = temp;
					isEmbeddedPool = true;
				}
			}
		}
		if (!pool.isRunning()) {
			throw new HoloClientException(ExceptionCode.ALREADY_CLOSE,
				"already close at " + pool.getCloseReasonStack().l, pool.getCloseReasonStack().r);
		}
		if (useFixedFe && fixedPool == null) {
			synchronized (this) {
				if (fixedPool == null) {
					ExecutionPool temp = new ExecutionPool("embedded-fixed-" + config.getAppName(), config, isShadingEnv, true);
					// 当useFixedFe为true时, 只会使用这个fixedPool注册的collector.
					collector = temp.register(this, config);
					fixedPool = temp;
					isEmbeddedFixedPool = true;
				}
			}
		}
		if (useFixedFe && !fixedPool.isRunning()) {
			throw new HoloClientException(ExceptionCode.ALREADY_CLOSE, "already close");
		}
	}

	public synchronized void setPool(ExecutionPool pool) throws HoloClientException {
		if (pool.isFixedPool()) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "fixed pool recived, require is not fixed");
		}
		ExecutionPool temp = pool;
		ActionCollector tempCollector = temp.register(this, config);
		if (!this.useFixedFe) {
			collector = tempCollector;
		}
		this.pool = temp;
		isEmbeddedPool = false;
	}

	public synchronized void setFixedPool(ExecutionPool fixedPool) throws HoloClientException {
		if (!useFixedFe || fixedPool == null || !fixedPool.isFixedPool()) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "fixedPool required not null, and enable HoloConfig:useFixedFe");
		}
		ExecutionPool tempFixedPool = fixedPool;
		this.fixedPool = tempFixedPool;
		collector = tempFixedPool.register(this, config);
		isEmbeddedFixedPool = false;
	}

	/**
	 * 根据配置选择使用fixed pool或者fe pool，仅在fixed fe支持的情况下使用.
	 */
	public synchronized ExecutionPool getExecPool() {
        return this.useFixedFe ? fixedPool : pool;
    }

	private void tryThrowException() throws HoloClientException {
		if (pool != null) {
			pool.tryThrowException();
		}
		if (fixedPool != null) {
			fixedPool.tryThrowException();
		}
	}

	/**
	 * 如果读写分区表，修改对应操作的schema为分区子表.
	 *
	 * @param record               操作的Record
	 * @param createIfNotExists    dynamicPartition为true，且是非delete的put操作时，自动创建分区
	 * @param exceptionIfNotExists 分区表不存在时是否抛出异常，get和delete操作发现子表不存在不会抛出异常
	 * @return 是否可以忽略本次操作，比如delete(PUT)但是分区子表不存在的时候；GET但分区子表不存在的时候
	 * @throws HoloClientException 获取分区或者根据分区信息获取TableSchema异常 那么complete exception
	 */
	private boolean rewriteForPartitionTable(Record record, boolean createIfNotExists, boolean exceptionIfNotExists) throws HoloClientException {
		TableSchema schema = record.getSchema();
		if (schema.isPartitionParentTable()) {
			boolean isStr = Types.VARCHAR == schema.getColumn(schema.getPartitionIndex()).getType() || Types.DATE == schema.getColumn(schema.getPartitionIndex()).getType();
			String value = String.valueOf(record.getObject(schema.getPartitionIndex()));
			Partition partition = pool.getOrSubmitPartition(schema.getTableNameObj(), value, isStr, createIfNotExists);
			if (partition != null) {
				TableSchema newSchema = pool.getOrSubmitTableSchema(TableName.quoteValueOf(partition.getSchemaName(), partition.getTableName()), false);
				record.changeToChildSchema(newSchema);
			} else if (exceptionIfNotExists) {
				throw new HoloClientWithDetailsException(ExceptionCode.TABLE_NOT_FOUND, "child table is not found", record);
			} else {
				return true;
			}
		}
		return false;
	}

	public void put(Put put) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		checkPut(put);
		ExecutionPool execPool = getExecPool();
		if (!rewriteForPartitionTable(put.getRecord(), config.isDynamicPartition() && !Put.MutationType.DELETE.equals(put.getRecord().getType()), !Put.MutationType.DELETE.equals(put.getRecord().getType()))) {
			if (!asyncCommit) {
				Record r = put.getRecord();
				PutAction action = new PutAction(Collections.singletonList(r), r.getByteSize(), config.getWriteMode(), BatchState.SizeEnough);
				while (!execPool.submit(action)) {

				}
				action.getResult();
			} else {
				collector.append(put.getRecord());
			}
		}
	}

	public CompletableFuture<Void> putAsync(Put put) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		checkPut(put);
		CompletableFuture<Void> ret = new CompletableFuture<>();
		if (!rewriteForPartitionTable(put.getRecord(), config.isDynamicPartition() && !Put.MutationType.DELETE.equals(put.getRecord().getType()), !Put.MutationType.DELETE.equals(put.getRecord().getType()))) {
			put.getRecord().setPutFuture(ret);
			collector.append(put.getRecord());
		} else {
			put.getRecord().setPutFuture(ret);
			ret.complete(null);
		}
		return ret;
	}

	public void put(List<Put> puts) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		HoloClientWithDetailsException detailException = null;
		List<Put> putList = new ArrayList<>();
		for (Put put : puts) {
			try {
				checkPut(put);
				if (!rewriteForPartitionTable(put.getRecord(), config.isDynamicPartition() && !Put.MutationType.DELETE.equals(put.getRecord().getType()), !Put.MutationType.DELETE.equals(put.getRecord().getType()))) {
					putList.add(put);
				}
			} catch (HoloClientWithDetailsException e) {
				if (detailException == null) {
					detailException = e;
				} else {
					detailException.merge(e);
				}
			}
		}
		for (Put put : putList) {
			collector.append(put.getRecord());
		}
		if (!asyncCommit) {
			collector.flush(false);
		}
		if (detailException != null) {
			throw detailException;
		}
	}

	/**
	 * 假设checkOp为GREATER
	 * 当checkValue不为null时, 用checkValue与表中checkColumn的当前值比较，满足checkOp时，则执行put，相当于sql `checkValue > old.column1`.
	 * 当checkValue为null时, 用Put中的新值与表中checkColumn当前值和进行比较，满足checkOp时，则执行put，相当于sql `excluded.column1 > old.column1`.
	 *
	 * @param checkColumn column
	 * @param checkOp     op
	 * @param checkValue  value
	 * @param nullValue   当前值为null时，视做nullValue。相当于sql `coalesce(old.column1, nullValue)`.
	 * @param put         put
	 * @throws HoloClientException e
	 */
	public void checkAndPut(String checkColumn, CheckCompareOp checkOp, Object checkValue, Object nullValue, Put put) throws HoloClientException {
		TableSchema schema = put.getRecord().getSchema();
		CheckAndPutRecord checkAndPutRecord = new CheckAndPutRecord(put.getRecord(),
				new CheckAndPutCondition(schema.getColumn(schema.getColumnIndex(checkColumn)), checkOp, checkValue, nullValue));

		checkAndPut(new CheckAndPut(checkAndPutRecord));
	}

	public void checkAndPut(String checkColumn, CheckCompareOp checkOp, Object checkValue, Put put) throws HoloClientException {
		checkAndPut(checkColumn, checkOp, checkValue, null, put);
	}

	/**
	 * 兼容beta版本接口. 不建议使用, checkValue会从put的record中获取, 只能每次单条写入无法攒批.
	 */
	public void checkAndPut(String checkColumn, CheckCompareOp checkOp, Put put) throws HoloClientException {
		checkAndPut(checkColumn, checkOp, put.getRecord().getObject(checkColumn), null, put);
	}

	public void checkAndPut(String checkColumn, Object nullValue, CheckCompareOp checkOp, Put put) throws HoloClientException {
		checkAndPut(checkColumn, checkOp, null, nullValue, put);
	}

	/**
	 * 直接传入CheckAndPut.
	 *
	 * @param put CheckAndPut
	 * @throws HoloClientException e
	 */
	public void checkAndPut(CheckAndPut put) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		checkCheckAndPut(put);
		final CheckAndPutRecord record = put.getRecord();

		ExecutionPool execPool = useFixedFe ? fixedPool : pool;
		if (!rewriteForPartitionTable(put.getRecord(), config.isDynamicPartition() && !Put.MutationType.DELETE.equals(put.getRecord().getType()), !Put.MutationType.DELETE.equals(put.getRecord().getType()))) {
			if (!asyncCommit) {
				PutAction action = new PutAction(Collections.singletonList(record), record.getByteSize(), config.getWriteMode(), BatchState.SizeEnough);

				while (!execPool.submit(action)) {

				}
				action.getResult();
			} else {
				collector.append(put.getRecord());
			}
		}
	}

	public ExportContext exportData(Exporter exporter) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		int threadSize = exporter.getThreadSize();
		int maxThread = this.config.readThreadSize > 1 ? this.config.readThreadSize - 1 : 1;
		if (threadSize > maxThread) {
			LOGGER.warn("Thread size is larger than max read thread size of holo client, will be using {}", maxThread);
			threadSize = maxThread;
		}
		int shardCount = Command.getShardCount(this, exporter.getSchema());
		int startShard = exporter.getStartShardId() == -1 ? 0 : exporter.getStartShardId();
		int endShard = exporter.getEndShardId() == -1 ? shardCount : exporter.getEndShardId();
		if (threadSize > (endShard - startShard)) {
			threadSize = endShard - startShard;
			LOGGER.warn("Thread size is larger than shard count, will be using thread size {}", threadSize);
		}

		OutputStream os = exporter.getOutputStream();
		InputStream[] istreams = new InputStream[threadSize];
		OutputStream[] ostreams = new OutputStream[threadSize];
		if (os == null) {
			for (int t = 0; t < threadSize; t++) {
				PipedOutputStream ostream = new InternalPipedOutputStream();
				PipedInputStream istream = new PipedInputStream();
				ostreams[t] = ostream;
				istreams[t] = istream;
				try {
					(ostream).connect(istream);
				} catch (IOException e) {
					throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "should not happen", e);
				}
			}
		} else {
			ostreams[0] = os;
		}

		int shardSize = (endShard - startShard) / threadSize;
		int remain = (endShard - startShard) % threadSize;
		CopyAction[] actions = new CopyAction[threadSize];
		for (int t = 0; t < threadSize; t++) {
			int end;
			if (remain > 0) {
				end = startShard + shardSize + 1;
				remain--;
			} else {
				end = startShard + shardSize;
			}
			CopyAction action = new CopyAction(exporter.getSchema(), ostreams[t], null, startShard, end, CopyAction.Mode.OUT);
			startShard = end;
			actions[t] = action;
			while (!pool.submit(action)) {

			}
		}

		try {
			CopyContext[] copyContexts = new CopyContext[threadSize];
			CompletableFuture<Long>[] futures = new CompletableFuture[threadSize];
			for (int t = 0; t < threadSize; t++) {
				copyContexts[t] = actions[t].getReadyToStart().get();
				futures[t] = actions[t].getFuture();
			}
			return new ExportContext(futures, copyContexts, istreams);

		} catch (InterruptedException e) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "interrupt", e);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof HoloClientException) {
				throw (HoloClientException) cause;
			} else {
				throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", cause);
			}
		}

	}

	public ImportContext importData(Importer importer) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		int threadSize = importer.getThreadSize();
		int maxThread = this.config.writeThreadSize > 1 ? this.config.writeThreadSize - 1 : 1;
		if (threadSize > maxThread) {
			LOGGER.warn("Thread size is larger than max write thread size of holo client, will be using {}", maxThread);
			threadSize = maxThread;
		}
		int shardCount = Command.getShardCount(this, importer.getSchema());
		int startShard = importer.getStartShardId() == -1 ? 0 : importer.getStartShardId();
		int endShard = importer.getEndShardId() == -1 ? shardCount : importer.getEndShardId();
		if (threadSize > (endShard - startShard)) {
			threadSize = endShard - startShard;
			LOGGER.warn("Thread size is larger than shard count, will be using thread size {}", threadSize);
		}
		InputStream is = importer.getInputStream();
		InputStream[] istreams = new InputStream[threadSize];
		OutputStream[] ostreams = new OutputStream[threadSize];
		if (is == null) {
			for (int t = 0; t < threadSize; t++) {
				PipedInputStream istream = new PipedInputStream(importer.getBufferSize() > 0 ? importer.getBufferSize() : 1024);
				PipedOutputStream ostream = new PipedOutputStream();
				istreams[t] = istream;
				ostreams[t] = ostream;
				try {
					ostream.connect(istream);
				} catch (IOException e) {
					throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "should not happen", e);
				}
			}
		} else {
			istreams[0] = is;
		}

		int shardSize = (endShard - startShard) / threadSize;
		int remain = (endShard - startShard) % threadSize;
		CopyAction[] actions = new CopyAction[threadSize];
		NavigableMap<Integer, Integer> shardMap = new TreeMap<>();
		for (int t = 0; t < threadSize; t++) {
			int end;
			if (remain > 0) {
				end = startShard + shardSize + 1;
				remain--;
			} else {
				end = startShard + shardSize;
			}
			CopyAction action = new CopyAction(importer.getSchema(), null, istreams[t], startShard, end, CopyAction.Mode.IN);
			action.setBufferSize(importer.getBufferSize());
			shardMap.put(startShard, t);
			startShard = end;
			actions[t] = action;
			while (!pool.submit(action)) {

			}

		}

		try {
			CopyContext[] copyContexts = new CopyContext[threadSize];
			CompletableFuture<Long>[] futures = new CompletableFuture[threadSize];
			for (int t = 0; t < threadSize; t++) {
				copyContexts[t] = actions[t].getReadyToStart().get();
				futures[t] = actions[t].getFuture();
			}
			return new ImportContext(shardMap, futures, copyContexts, ostreams, shardCount);
		} catch (InterruptedException e) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "interrupt", e);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof HoloClientException) {
				throw (HoloClientException) cause;
			} else {
				throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", cause);
			}
		}
	}

	/**
	 * 检查传入的binlog offset map是否合法.
	 */
	private Map<Integer, BinlogOffset> checkBinlogOffsetMap(Subscribe subscribe, int shardCount) throws HoloClientException {
		Map<Integer, BinlogOffset> offsetMap = subscribe.getOffsetMap();
		if (offsetMap == null && subscribe.getBinlogReadStartTime() == null) {
			// 至少得设置一个
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("at least one of offsetMap and binlogReadStartTime must be set for subscribe %s", subscribe));
		}
		// 如果只设置了起始时间, 默认为所有shard初始化offset
		if (offsetMap == null) {
			offsetMap = new HashMap<>();
			for (int i = 0; i < shardCount; i++) {
				offsetMap.put(i, new BinlogOffset().setTimestamp(subscribe.getBinlogReadStartTime()));
			}
		}
		for (Integer shardId : offsetMap.keySet()) {
			if (shardId < 0 || shardId >= shardCount) {
				throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("invalid shard id [%s] for table %s", shardId, subscribe.getTableName()));
			}
		}
		return offsetMap;
	}

	public BinlogShardGroupReader binlogSubscribe(Subscribe subscribe) throws HoloClientException {
		ensurePoolOpen();
		if (config.getBinlogReadTimeoutMs() > 0 && config.getBinlogReadTimeoutMs() < config.getBinlogHeartBeatIntervalMs()) {
			// 心跳时间间隔必须小于超时时间
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, "Subscribe binlog need set BinlogReadTimeoutSeconds greater than BinlogHeartBeatIntervalMs.");
		}
		TableSchemaSupplier supplier = new TableSchemaSupplier() {
			@Override
			public TableSchema apply() throws HoloClientException {
				return HoloClient.this.getTableSchema(subscribe.getTableName(), true);
			}
		};
		TableSchema schema = supplier.apply();
		int shardCount = Command.getShardCount(this, schema);
		if (subscribe.getSlotName() != null && !Command.getSlotNames(this, schema).contains(subscribe.getSlotName())) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("The table %s has no slot named %s", schema.getTableNameObj().getFullName(), subscribe.getSlotName()));
		}
		if (subscribe.getSlotName() == null) {
			HoloVersion holoVersion = Command.getHoloVersion(this);
			if (holoVersion.compareTo(new HoloVersion("2.1.0")) < 0) {
				throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("For hologres instance version lower than r2.1.0, need to provide slotName to subscribe binlog. your version is %s", holoVersion));
			}
		}
		Map<Integer, BinlogOffset> offsetMap = checkBinlogOffsetMap(subscribe, shardCount);

		BinlogShardGroupReader reader = null;
		try {
			AtomicBoolean started = new AtomicBoolean(true);
			Map<Integer, Committer> committerMap = new HashMap<>();
			BinlogRecordCollector collector = new BinlogRecordCollector(new ArrayBlockingQueue<>(Math.max(1024, offsetMap.size() * config.getBinlogReadBatchSize() / 2)));
			reader = new BinlogShardGroupReader(config, subscribe, committerMap, started, collector);
			for (Map.Entry<Integer, BinlogOffset> entry : offsetMap.entrySet()) {
				BlockingQueue<Tuple<CompletableFuture<Void>, Long>> commiterJobQueue = new ArrayBlockingQueue<>(1);
				Committer committer = new Committer(commiterJobQueue);
				committerMap.put(entry.getKey(), committer);
				BinlogAction action = new BinlogAction(subscribe.getTableName(), subscribe.getSlotName(), entry.getKey(), entry.getValue().getSequence(), entry.getValue().getStartTimeText(), reader.getCollector(), supplier, commiterJobQueue);
				reader.addThread(getExecPool().submitOneShotAction(started, String.format("binlog-%s-%s", subscribe.getTableName(), entry.getKey()), action));
			}
		} catch (HoloClientException e) {
            reader.close();
            throw e;
		}
		return reader;
	}

	/**
	 * 验证分区订阅配置是否有效.
	 *	检查配置是否符合要求,需要消费的分区是否存在,分期需要消费的shard是否正确等
	 *
	 * @param partitionedSubscribe 父级订阅对象
	 * @throws HoloClientException 如果验证失败，则抛出此异常
	 * @return 必要的话, 会对分区订阅对象进行调整
	 */
	private Subscribe validatePartitionSubscription(Subscribe partitionedSubscribe) throws HoloClientException {
		TableSchema parentSchema = getTableSchema(partitionedSubscribe.getTableName());
		if (!parentSchema.isPartitionParentTable()) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("The table %s is not a partition parent table.", partitionedSubscribe.getTableName()));
		}
		if (config.getBinlogPartitionSubscribeMode() == BinlogPartitionSubscribeMode.DISABLE) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("Subscribe partition table %s binlog should set BinlogPartitionSubscribeMode.", partitionedSubscribe.getTableName()));
		}
		HoloVersion holoVersion = Command.getHoloVersion(this);
		// 消费分区父表binlog,可能占用较多的连接数,建议2.1.20版本以上结合fixed fe模式来使用.
		if (holoVersion.compareTo(new HoloVersion("2.1.20")) < 0) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("Subscription partition parent table requires the hologres instance version greater than 2.1.20. your version is %s", holoVersion));
		}
		// 消费分区父表需要设置心跳时间间隔
		if (config.getBinlogHeartBeatIntervalMs() <= 0) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("Subscribe partition parent table %s binlog need set BinlogHeartBeatIntervalMs.", partitionedSubscribe.getTableName()));
		}
		// 心跳时间间隔必须小于超时时间
		if (config.getBinlogReadTimeoutMs() > 0 && config.getBinlogReadTimeoutMs() < config.getBinlogHeartBeatIntervalMs()) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, "Subscribe binlog need set BinlogReadTimeoutSeconds greater than BinlogHeartBeatIntervalMs.");
		}
		NavigableMap<String, Subscribe> partitionToSubscribe = partitionedSubscribe.getPartitionToSubscribeMap();
		int shardCount = Command.getShardCount(this, parentSchema);
		Map<Integer, BinlogOffset> offsetMap = checkBinlogOffsetMap(partitionedSubscribe, shardCount);
		Set<Integer> shardIds = offsetMap.keySet();
		LinkedHashMap<String, TableName> partValueToPartitionTableMap = Command.getPartValueToPartitionTableMap(this, parentSchema);
		if (config.getBinlogPartitionSubscribeMode() == BinlogPartitionSubscribeMode.STATIC) {
			// STATIC mode
			for (Subscribe subscribe : partitionToSubscribe.values()) {
				TableName partitionName = TableName.valueOf(subscribe.getTableName());
				if (!partValueToPartitionTableMap.containsValue(partitionName)) {
					throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("partition table %s not a child table of %s.", partitionName, partitionedSubscribe.getTableName()));
				}
				if (!subscribe.getOffsetMap().keySet().equals(shardIds)) {
					throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("shardIds %s to subscribe of partition table %s not same with parent table %s binlog offset shardIds %s.",
							offsetMap.keySet(),
							subscribe.getTableName(),
							partitionedSubscribe.getTableName(),
							shardIds));
				}
			}
		} else if (config.getBinlogPartitionSubscribeMode() == BinlogPartitionSubscribeMode.DYNAMIC) {
			// DYNAMIC模式需要开启自动分区, 且需要设置预创建分区数量大于等于1
			if (!parentSchema.getAutoPartitioning().isEnable()) {
				throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("Subscribe partition parent table %s binlog by DYNAMIC mode need enable auto partitioning.", partitionedSubscribe.getTableName()));
			}
			if (parentSchema.getAutoPartitioning().getPreCreateNum() < 1) {
                throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("Subscribe partition parent table %s binlog by DYNAMIC mode need set PreCreateNum lager than 1, but now is %s.",
						partitionedSubscribe.getTableName(),
						parentSchema.getAutoPartitioning().getPreCreateNum()));
            }
			// DYNAMIC模式有状态恢复,最多只能指定2张表,在消费延迟数据时,可能存在两张表同时保存checkpoint的情况
			if (partitionToSubscribe.size() > 2) {
				throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("Subscribe partition parent table %s binlog by DYNAMIC mode," +
						" need to specify at most two partition tables, but now is %s.", partitionedSubscribe.getTableName(), partitionToSubscribe.size()));
			}
			// 考虑用户传入的表可能是较早的分区子表，已经被删除
			Iterator<Map.Entry<String, Subscribe>> iterator = partitionToSubscribe.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<String, Subscribe> entry = iterator.next();
				if (!partValueToPartitionTableMap.containsValue(TableName.valueOf(entry.getKey()))) {
					boolean hasLsn = entry.getValue().getOffsetMap().values().stream()
							.anyMatch(BinlogOffset::hasSequence);
					if (hasLsn) {
						throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("partition table %s not found, it may be an earlier partition table that has been dropped, could not start subscribe from lsn.", entry.getKey()));
					} else {
						LOGGER.warn("partition table {} not found, it may be an earlier partition table that has been dropped, ignore it.", entry.getKey());
						iterator.remove();
					}
				}
			}
			// 传入的子表都已经过期被删除了，消费默认从目前最早的分区子表开始
			if (partitionToSubscribe.isEmpty()) {
				Iterator<Map.Entry<String, TableName>> iter = partValueToPartitionTableMap.entrySet().iterator();
				TableName oldestPartition = null;
				while (iter.hasNext()) {
					TableName tableName = iter.next().getValue();
					if (PartitionUtil.isPartitionTableNameLegal(tableName.getTableName(), parentSchema.getAutoPartitioning())) {
						oldestPartition = tableName;
						break;
					} else {
						// 分区子表表名不符合规则, 忽略
						LOGGER.warn("partition table {} is illegal, ignore it.", tableName);
					}
				}
				if (oldestPartition == null) {
                    throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("partition parent table %s has no valid partition table.", partitionedSubscribe.getTableName()));
                }
				// 继承部分传入的父表订阅信息, 重新构建
				Subscribe.OffsetBuilder newBuilder = Subscribe.newOffsetBuilder(partitionedSubscribe.getTableName());
				for (Map.Entry<Integer, BinlogOffset> entry : offsetMap.entrySet()) {
					newBuilder.addShardStartOffset(entry.getKey(), entry.getValue());
				}
				newBuilder.addShardsStartOffsetForPartition(oldestPartition.getFullName(), shardIds, new BinlogOffset());
				partitionedSubscribe = newBuilder.build();
				LOGGER.info("start binlog subscribe from oldest partition {} by default.", oldestPartition);
			}
		}
		return partitionedSubscribe;
	}

	/**
	 * 订阅分区表binlog.
	 * 	1. 如果未指定子表的订阅信息,则根据父表订阅信息选择需要消费的子表
	 * 		1.1 STATIC mode: 从参数获取需要消费的子表,或者消费所有子表
	 * 		1.2 DYNAMIC mode: 从父表订阅信息中获取最早的起始时间,选择此时间对应的子表开始消费
	 * 	2. 验证分区订阅配置是否有效
	 * 	3. 创建分区父表reader, 启动对分区binlog的读取
	 *
	 * @param partitionedSubscribe 分区父表的订阅信息
	 * @return  返回分区父表binlog的reader, 内部可能消费多个子表
	 */
	public BinlogPartitionGroupReader partitionBinlogSubscribe(Subscribe partitionedSubscribe) throws HoloClientException {
		ensurePoolOpen();
		TableSchema parentSchema = this.getTableSchema(partitionedSubscribe.getTableName());
		int shardCount = Command.getShardCount(this, parentSchema);
		Map<Integer, BinlogOffset> offsetMap = checkBinlogOffsetMap(partitionedSubscribe, shardCount);
		Set<Integer> shards = offsetMap.keySet();

		// 没有特别指定子表的订阅信息
		if (partitionedSubscribe.getPartitionToSubscribeMap().isEmpty()) {
			// 继承部分传入的父表订阅信息, 重新构建
			Subscribe.OffsetBuilder newBuilder = Subscribe.newOffsetBuilder(partitionedSubscribe.getTableName());
			for (Map.Entry<Integer, BinlogOffset> entry : offsetMap.entrySet()) {
				newBuilder.addShardStartOffset(entry.getKey(), entry.getValue());
            }
			if (config.getBinlogPartitionSubscribeMode() == BinlogPartitionSubscribeMode.STATIC) {
				// STATIC mode: 从参数获取需要消费的子表,或者消费所有子表
				LinkedHashMap<String, TableName> partValueToPartitionTableMap = Command.getPartValueToPartitionTableMap(this, parentSchema);
				if (config.getPartitionValuesToSubscribe() != null && config.getPartitionValuesToSubscribe().length > 0) {
					// 通过指定的分区值,计算消费指定的子表
					String[] partitionTables = new String[config.getPartitionValuesToSubscribe().length];
					for (int i = 0; i < config.getPartitionValuesToSubscribe().length; i++) {
						String partValue = config.getPartitionValuesToSubscribe()[i];
						if (partValueToPartitionTableMap.containsKey(partValue)) {
							partitionTables[i] = partValueToPartitionTableMap.get(partValue).getFullName();
						} else {
							throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("partition value %s not found in partitioned table %s.", partValue, parentSchema.getTableName()));

						}
					}
					for (Map.Entry<Integer, BinlogOffset> entry : offsetMap.entrySet()) {
						int shard = entry.getKey();
						long startTime = entry.getValue().getTimestamp();
						// 每个子表都从父表对应shard指定的消费时间开始消费
						for (String partitionTable : partitionTables) {
							newBuilder.addShardStartOffsetForPartition(partitionTable, shard, new BinlogOffset().setTimestamp(startTime));
						}
					}
				} else {
					//  未指定子表,默认消费所有子表
					for (Map.Entry<Integer, BinlogOffset> entry :offsetMap.entrySet()) {
						int shard = entry.getKey();
						long startTime = entry.getValue().getTimestamp();
						// 每个子表都从父表对应shard指定的消费时间开始消费
						for (TableName partitionName : partValueToPartitionTableMap.values()) {
							newBuilder.addShardStartOffsetForPartition(partitionName.getFullName(), shard, new BinlogOffset().setTimestamp(startTime));
						}
					}
				}
			} else {
				// DYNAMIC mode: 从父表订阅信息中获取时间,选择此时间对应的子表开始消费
				// 获取最早的时间: 我们认为指定时间消费的场景,所有shard的时间应该都是相同的,这里取最早的时间并计算对应的分区,作为相应子表所有shard的开始分区及开始时间
				long startTime = Long.MAX_VALUE;
				for (BinlogOffset binlogOffset : offsetMap.values()) {
					startTime = Math.min(startTime, binlogOffset.getTimestamp());
				}
				ZonedDateTime z = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTime / 1000L), parentSchema.getAutoPartitioning().getTimeZoneId());
				TableName earliestPartition = TableName.quoteValueOf(parentSchema.getSchemaName(),
								PartitionUtil.getPartitionNameByDateTime(parentSchema.getTableName(), z, parentSchema.getAutoPartitioning()));
				LOGGER.info("start binlog subscribe from partition {}, startTime {}", earliestPartition, z);
				newBuilder.addShardsStartOffsetForPartition(earliestPartition.getFullName(), shards, new BinlogOffset().setTimestamp(startTime));
			}
			partitionedSubscribe = newBuilder.build();
        }

		// 验证分区订阅配置是否有效
		partitionedSubscribe = validatePartitionSubscription(partitionedSubscribe);

		AtomicBoolean started = new AtomicBoolean(true);
		BinlogRecordCollector collector = new BinlogRecordCollector(new ArrayBlockingQueue<>(Math.max(1024, shards.size() * config.getBinlogReadBatchSize() * 5)));
		BinlogPartitionGroupReader reader = new BinlogPartitionGroupReader(config, partitionedSubscribe, started, parentSchema, this, collector);
		for (Subscribe subscribe : partitionedSubscribe.getPartitionToSubscribeMap().values()) {
			reader.startPartitionSubscribe(TableName.valueOf(subscribe.getTableName()), subscribe);
		}
		return reader;
	}

	public void flush() throws HoloClientException {
		ensurePoolOpen();
		collector.flush(false);
	}

	public boolean isAsyncCommit() {
		return asyncCommit;
	}

	public void setAsyncCommit(boolean asyncCommit) {
		this.asyncCommit = asyncCommit;
	}

	private void closeInternal() throws HoloClientException{
		HoloClientException exception = null;
		if (pool != null && pool.isRegister(this)) {
			try {
				tryThrowException();
				flush();
			} catch (HoloClientException e) {
				LOGGER.error("fail when close", e);
				exception = e;
			}
			pool.unregister(this);
			if (isEmbeddedPool) {
				pool.close();
			}
		}
		if (fixedPool != null && fixedPool.isRegister(this)) {
			fixedPool.unregister(this);
			if (isEmbeddedFixedPool) {
				fixedPool.close();
			}
		}
		if (null != exception) {
			throw exception;
		}
	}

	@Override
	public void close() {
		try {
			closeInternal();
		} catch (HoloClientException e) {
			throw new RuntimeException(e);
		}
	}
}
