/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.impl.action.CopyAction;
import com.alibaba.hologres.client.impl.action.MetaAction;
import com.alibaba.hologres.client.impl.action.PutAction;
import com.alibaba.hologres.client.impl.action.ScanAction;
import com.alibaba.hologres.client.impl.action.SqlAction;
import com.alibaba.hologres.client.impl.collector.ActionCollector;
import com.alibaba.hologres.client.impl.collector.BatchState;
import com.alibaba.hologres.client.impl.copy.CopyContext;
import com.alibaba.hologres.client.impl.copy.InternalPipedOutputStream;
import com.alibaba.hologres.client.model.BinlogOffset;
import com.alibaba.hologres.client.model.ExportContext;
import com.alibaba.hologres.client.model.ImportContext;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordScanner;
import org.postgresql.core.SqlCommandType;
import org.postgresql.model.Column;
import org.postgresql.model.Partition;
import org.postgresql.model.TableName;
import org.postgresql.model.TableSchema;
import org.postgresql.util.FunctionWithSQLException;
import org.postgresql.util.IdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

	private ActionCollector collector;

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

	public HoloClient(HoloConfig config) throws HoloClientException {
		String url = config.getJdbcUrl();
		try {
			Class.forName("com.alibaba.hologres.org.postgresql.Driver");
			isShadingEnv = true;
		} catch (Exception e) {
			try {
				Class.forName("org.postgresql.Driver");
			} catch (Exception e2) {
				throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "load driver fail", e);
			}
		}
		checkConfig(config);
		this.config = config;
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
		return doGetTableSchema(tableName, noCache).getResult();
	}

	public CompletableFuture<TableSchema> getTableSchemaAsync(TableName tableName, boolean noCache) throws HoloClientException {
		return doGetTableSchema(tableName, noCache).getFuture();
	}

	private MetaAction doGetTableSchema(TableName tableName, boolean noCache) throws HoloClientException {
		ensurePoolOpen();
		return pool.getOrSubmitTableSchema(tableName, noCache);
	}

	private void checkGet(Get get) throws HoloClientException {
		if (get == null) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, "Get cannot be null");
		}
		if (get.getRecord().getSchema().getPrimaryKeys().length == 0) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, "Get table must have primary key:" + get.getRecord().getSchema().getTableNameObj().getFullName());
		}

		for (int index : get.getRecord().getKeyIndex()) {
			if (!get.getRecord().isSet(index) || null == get.getRecord().getObject(index)) {
				throw new HoloClientException(ExceptionCode.INVALID_REQUEST, "Get primary key cannot be null:" + get.getRecord().getSchema().getColumnSchema()[index].getName());
			}
		}
	}

	private void checkPut(Put put) throws HoloClientWithDetailsException {
		if (put == null) {
			throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "Put cannot be null", put.getRecord());
		}
		for (int index : put.getRecord().getKeyIndex()) {
			if ((!put.getRecord().isSet(index) || null == put.getRecord().getObject(index)) && put.getRecord().getSchema().getColumn(index).getDefaultValue() == null) {
				throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "Put primary key cannot be null:" + put.getRecord().getSchema().getColumnSchema()[index].getName(), put.getRecord());
			}
		}
		if (put.getRecord().getSchema().isPartitionParentTable() && (!put.getRecord().isSet(put.getRecord().getSchema().getPartitionIndex()) || null == put.getRecord().getObject(put.getRecord().getSchema().getPartitionIndex()))) {
			throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "Put partition key cannot be null:" + put.getRecord().getSchema().getColumnSchema()[put.getRecord().getSchema().getPartitionIndex()].getName(), put.getRecord());

		}
		if ((put.getRecord().getType() == SqlCommandType.UPDATE || put.getRecord().getType() == SqlCommandType.DELETE) && put.getRecord().getSchema().getPrimaryKeys().length == 0) {
			throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "Delete/Update Put table must have primary key:" + put.getRecord().getSchema().getTableNameObj().getFullName(), put.getRecord());
		}
		if (put.getRecord().getType() == SqlCommandType.UPDATE) {
			int columnSize = put.getRecord().getSchema().getColumnSchema().length;
			int updateColumnSize = 0;
			for (int i = 0; i < columnSize; ++i) {
				Column column = put.getRecord().getSchema().getColumnSchema()[i];
				if (put.getRecord().isSet(i) && !column.getPrimaryKey()) {
					++updateColumnSize;
				}
			}
			if (updateColumnSize == 0) {
				throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "Update Put must contain non primary key column" + put.getRecord(), put.getRecord());
			}
		}
	}

	public CompletableFuture<Record> get(Get get) throws HoloClientException {
		ensurePoolOpen();
		checkGet(get);
		collector.appendGet(get);
		return get.getFuture();
	}

	public List<CompletableFuture<Record>> get(List<Get> gets) throws HoloClientException {
		ensurePoolOpen();
		for (Get get : gets) {
			checkGet(get);
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
		while (!pool.submit(action)) {

		}
		return action;
	}

	private void ensurePoolOpen() throws HoloClientException {
		if (pool == null) {
			synchronized (this) {
				if (pool == null) {
					ExecutionPool temp = new ExecutionPool("embedded-" + config.getAppName(), config, isShadingEnv);
					collector = temp.register(this, config);
					pool = temp;
					isEmbeddedPool = true;
				}
			}
		}
		if (!pool.isRunning()) {
			throw new HoloClientException(ExceptionCode.ALREADY_CLOSE, "already close");
		}
	}

	public synchronized void setPool(ExecutionPool pool) throws HoloClientException {
		ExecutionPool temp = pool;
		collector = temp.register(this, config);
		this.pool = temp;
		isEmbeddedPool = false;
	}

	private void tryThrowException() throws HoloClientException {
		if (pool != null) {
			pool.tryThrowException();
		}
	}

	/**
	 * 如果put为分区表，修改put的schema为分区字表.
	 * dynamicPartition=false 且 分区子表不存在则报错
	 *
	 * @param put put
	 * @throws HoloClientException
	 */
	private void rewritePut(Put put) throws HoloClientException {
		Record record = put.getRecord();
		TableSchema schema = record.getSchema();
		if (record.getSchema().getPartitionIndex() > -1 && config.isEnableClientDynamicPartition()) {
			boolean dynamicPartition = config.isDynamicPartition();
			boolean isStr = Types.VARCHAR == schema.getColumn(schema.getPartitionIndex()).getType();
			String value = String.valueOf(record.getObject(schema.getPartitionIndex()));
			Partition partition = pool.getOrSubmitPartition(schema.getTableNameObj(), value, isStr, dynamicPartition && !SqlCommandType.DELETE.equals(put.getRecord().getType()));
			if (partition != null) {
				TableSchema newSchema = pool.getOrSubmitTableSchema(TableName.valueOf(IdentifierUtil.quoteIdentifier(partition.getSchemaName(), true), IdentifierUtil.quoteIdentifier(partition.getTableName(), true)), false).getResult();
				record.changeToChildSchema(newSchema);
			} else if (!SqlCommandType.DELETE.equals(put.getRecord().getType())) {
				throw new HoloClientWithDetailsException(ExceptionCode.INVALID_REQUEST, "child table is not found", record);
			}
		}
	}

	public void put(Put put) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		checkPut(put);
		rewritePut(put);
		if (!asyncCommit) {
			Record r = put.getRecord();
			PutAction action = new PutAction(Collections.singletonList(r), r.getByteSize(), BatchState.SizeEnough);
			while (!pool.submit(action)) {

			}
			action.getResult();
		} else {
			collector.append(put.getRecord());
		}

	}

	public CompletableFuture<Void> putAsync(Put put) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		checkPut(put);
		rewritePut(put);
		CompletableFuture<Void> ret = new CompletableFuture<>();
		put.getRecord().setPutFuture(ret);
		collector.append(put.getRecord());
		return ret;
	}

	public void put(List<Put> puts) throws HoloClientException {
		ensurePoolOpen();
		tryThrowException();
		for (Put put : puts) {
			checkPut(put);
			rewritePut(put);
		}
		for (Put put : puts) {
			collector.append(put.getRecord());
		}
		if (!asyncCommit) {
			collector.flush(false);
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

	public ConnectionHolder createConnectionHolder(Properties info) {
		if (info != null) {
			return new ConnectionHolder(config, this, isShadingEnv, info);
		} else {
			return new ConnectionHolder(config, this, isShadingEnv);
		}
	}

	public ConnectionHolder createConnectionHolder() {
		return new ConnectionHolder(config, this, isShadingEnv);
	}

	public BinlogShardGroupReader binlogSubscribe(TableSchema schema, Map<Integer, BinlogOffset> offsetMap) throws HoloClientException, SQLException, IOException {
		int shardCount = Command.getShardCount(this, schema);
		if (offsetMap.size() != shardCount) {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, String.format("The offsetMap size %s not equal to shardCount %s", offsetMap.size(), shardCount));
		}
		String slotName = Command.getSlotName(this, schema);
		return BinlogShardGroupReader.newBuilder(this, schema, config).setShardRange(0, shardCount).setSlotName(slotName).setBinlogOffsetMap(offsetMap).build();
	}

	public BinlogShardGroupReader binlogSubscribe(TableSchema schema) throws HoloClientException, SQLException, IOException {
		int shardCount = Command.getShardCount(this, schema);
		String slotName = Command.getSlotName(this, schema);
		return BinlogShardGroupReader.newBuilder(this, schema, config).setShardRange(0, shardCount).setSlotName(slotName).build();
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

	private void closeInternal() {

		if (pool != null && pool.isRegister(this)) {
			try {
				tryThrowException();
				flush();
			} catch (HoloClientException e) {
				LOGGER.error("fail when close", e);
			}
			pool.unregister(this);
			if (isEmbeddedPool) {
				pool.close();
			}
		}
	}

	@Override
	public void close() {
		closeInternal();
	}
}
