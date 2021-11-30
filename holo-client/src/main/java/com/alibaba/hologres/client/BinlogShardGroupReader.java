package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.model.BinlogOffset;
import com.alibaba.hologres.client.model.Record;
import org.postgresql.model.TableSchema;
import org.postgresql.util.HoloVersion;
import org.postgresql.util.MetaUtil;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BinlogShardGroupReader 为一个范围的shard创建BinlogShardReader，并将各个reader返回的Record放入queue.
 */
public class BinlogShardGroupReader {
	public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BinlogShardGroupReader.class);

	private final HoloClient client;
	private final TableSchema schema;
	private final HoloConfig config;
	private final int startShardId;
	private final int endShardId;
	private final String slotName;
	private final Map<Integer, BinlogOffset> offsetMap;
	private HoloVersion holoVersion;

	public BinlogShardGroupReader(HoloClient client, TableSchema schema, HoloConfig config, int startShardId, int endShardId, String slotName, Map<Integer, BinlogOffset> offsetMap) throws HoloClientException {
		this.client = client;
		this.schema = schema;
		this.startShardId = startShardId;
		this.endShardId = endShardId;
		this.config = config;
		this.slotName = slotName;
		this.offsetMap = offsetMap;

		// Check hologres version
		ConnectionHolder connectionHolder = client.createConnectionHolder();
		connectionHolder.retryExecute((conn) -> {
			holoVersion = MetaUtil.getHoloVersion(conn);
			return null;
		}, 1);
		connectionHolder.close();

		if (holoVersion.compareTo(new HoloVersion(1, 1, 2)) < 0) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "Binlog reader need hologres version >= 1.1.2 ");
		}

		this.threadSize = endShardId - startShardId;
		this.numOpened = new AtomicInteger(threadSize);
		this.queue = new ArrayBlockingQueue<Record>(1024);

		this.start();
	}

	int threadSize;
	AtomicInteger numOpened;
	BlockingQueue<Record> queue;
	ExecutorService threadPool = Executors.newCachedThreadPool();

	/**
	 * Call getRecord until null or call BinlogShardGroupReader.cancel() to interrupt.
	 */
	public Record getRecord() {
		while (numOpened.get() > 0 || !queue.isEmpty()) {
			Record r;
			try {
				r = queue.poll(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				return null;
			}
			if (r != null) {
				return r;
			}
		}
		threadPool.shutdown();
		return null;
	}

	private void start() throws HoloClientException {
		for (int i = startShardId; i < endShardId; i++) {
			LOGGER.info("Create BinlogShardReader, shardId: {} ", i);
			threadPool.execute(new BinlogShardReader(client, schema, config, slotName, queue, i, (offsetMap == null) ? new BinlogOffset() : offsetMap.get(i)));
		}
	}

	public void cancel() {
		threadPool.shutdownNow();
		numOpened.set(0);
	}

	/**
	 * Cancel after waitMs.
	 */
	public void cancel(long waitMs) throws InterruptedException {
		Thread.sleep(waitMs);
		cancel();
	}

	public boolean isCanceled() {
		return threadPool.isTerminated();
	}

	public static BinlogShardGroupReader.Builder newBuilder(HoloClient client, TableSchema schema, HoloConfig config) {
		return new BinlogShardGroupReader.Builder(client, schema, config);
	}

	/**
	 * builder.
	 */
	public static class Builder {
		private final HoloClient client;
		private final TableSchema schema;
		private final HoloConfig config;
		private int startShardId = -1;
		private int endShardId = -1;
		private String slotName;
		private Map<Integer, BinlogOffset> offsetMap;

		public Builder(HoloClient client, TableSchema schema, HoloConfig config) {
			this.client = client;
			this.schema = schema;
			this.config = config;
		}

		public BinlogShardGroupReader.Builder setSlotName(String slotName) {
			this.slotName = slotName;
			return this;
		}

		public BinlogShardGroupReader.Builder setBinlogOffsetMap(Map<Integer, BinlogOffset> offsetMap) {
			this.offsetMap = offsetMap;
			return this;
		}

		/**
		 * 输出shardId [start,end) 的数据.
		 *
		 * @param startShardId start
		 * @param endShardId   end
		 * @return
		 */
		public BinlogShardGroupReader.Builder setShardRange(int startShardId, int endShardId) {
			if (endShardId <= startShardId) {
				throw new InvalidParameterException("startShardId must less then endShardId");
			}
			this.startShardId = startShardId;
			this.endShardId = endShardId;
			return this;
		}

		public BinlogShardGroupReader build() throws IOException, HoloClientException, SQLException {
			return new BinlogShardGroupReader(client, schema, config, startShardId, endShardId, slotName, offsetMap);
		}
	}
}
