package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.binlog.HoloBinlogDecoder;
import com.alibaba.hologres.client.model.BinlogOffset;
import com.alibaba.hologres.client.model.Record;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.model.TableSchema;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * BinlogShardReader，读取某个shard的binlog并解析为record，put到queue中.
 */
public class BinlogShardReader implements Runnable {
	public static final Logger LOGGER = LoggerFactory.getLogger(BinlogShardReader.class);

	private final HoloClient client;
	private final TableSchema schema;
	private final HoloConfig config;
	private final String slotName;
	private final BlockingQueue<Record> queue;
	private final int shardId;
	private BinlogOffset binlogOffset;
	private long startLsn;
	private String startTime;

	public BinlogShardReader(HoloClient client, TableSchema schema, HoloConfig config, String slotName, BlockingQueue<Record> queue, int shardId, BinlogOffset binlogOffset) throws HoloClientException {
		this.client = client;
		this.schema = schema;
		this.config = config;
		this.slotName = slotName;
		this.queue = queue;
		this.shardId = shardId;
		if (binlogOffset != null) {
			this.binlogOffset = binlogOffset;
		}
		init();
	}

	private HoloBinlogDecoder decoder;
	private long timeoutMs;
	private PGReplicationStream pgReplicationStream;
	private ByteBuffer byteBuffer;
	private long readSucceedTime = System.currentTimeMillis();
	private PgConnection conn;

	private void init() throws HoloClientException {
		this.decoder = new HoloBinlogDecoder(client, schema, shardId, config.getBinlogIgnoreDelete(), config.getBinlogIgnoreBeforeUpdate());
		this.timeoutMs = config.getBinlogReadTimeoutSeconds() * 1000L;

		Properties info = new Properties();
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(info, "9.4");
		PGProperty.REPLICATION.set(info, "database");
		PGProperty.TCP_KEEP_ALIVE.set(info, true);

		// Replication Connection 不能执行其他sql，因此单独创建 Replication Connection.
		ConnectionHolder connectionHolder = client.createConnectionHolder(info);

		startLsn = config.getBinlogReadStartLsn();
		startTime = config.getBinlogReadStartTime();
		// 如果设置了binlogOffset，则从binlogOffset中获取启动位点，且lsn优先于timestamp
		if (!binlogOffset.isInvalid()){
			if (binlogOffset.hasSequence()) {
				startLsn = binlogOffset.getSequence();
				LOGGER.info("Create BinlogShardReader from BinlogOffset with the startLsn: {} ", startLsn);
			} else if (binlogOffset.hasTimestamp()){
				startTime = (new Timestamp(binlogOffset.getTimestamp())).toString();
				LOGGER.info("Create BinlogShardReader from BinlogOffset with the startTime: {} ", startTime);
			}
		}

		connectionHolder.retryExecute((conn) -> {
			this.conn = conn;
			this.pgReplicationStream = this.conn
					.getReplicationAPI()
					.replicationStream()
					.logical()
					.withSlotName(slotName)
					.withSlotOption("parallel_index", shardId)
					.withSlotOption("start_lsn",  String.valueOf(startLsn))
					.withSlotOption("start_time", startTime)
					.withSlotOption("batch_size", config.getBinlogReadBatchSize())
					.withStatusInterval(5, TimeUnit.SECONDS)
					.start();
			return null;
		}, 1);

	}

	public List<Record> getRecords() throws HoloClientException, IOException {
		read();
		while (fetchRecords().size() == 0) {
			read();
		}

		return fetchRecords();
	}

	private List<Record> fetchRecords() throws IOException, HoloClientException {
		List<Record> records = decoder.decode(byteBuffer);
		byteBuffer.clear();

		return records;
	}

	private void read() throws HoloClientException {
		try {
			byteBuffer = pgReplicationStream.readPending();
			while (byteBuffer == null) {
				Thread.sleep(1000);

				long currentTime = System.currentTimeMillis();
				if (currentTime - readSucceedTime > timeoutMs) {
					throw new TimeoutException("read Binlog timeout after " + timeoutMs + "ms");
				}
				byteBuffer = pgReplicationStream.readPending();
			}
		} catch (SQLException | InterruptedException | TimeoutException e) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "Read failed: PgReplicationStream have been closed" , e);
		}
		readSucceedTime = System.currentTimeMillis();
	}

	@Override
	public void run() {
		try {
			List<Record> records;
			long lastLsn;
			while ((records = getRecords()) != null) {
				for (Record record : records) {
					lastLsn = record.getBinlogParams()[0];
					queue.put(record);
					pgReplicationStream.setFlushedLSN(LogSequenceNumber.valueOf(lastLsn));
				}
				pgReplicationStream.forceUpdateStatus();
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}
}
