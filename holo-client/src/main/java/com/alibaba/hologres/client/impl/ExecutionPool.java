/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.action.AbstractAction;
import com.alibaba.hologres.client.impl.action.GetAction;
import com.alibaba.hologres.client.impl.action.MetaAction;
import com.alibaba.hologres.client.impl.action.PutAction;
import com.alibaba.hologres.client.impl.action.ScanAction;
import com.alibaba.hologres.client.impl.action.SqlAction;
import com.alibaba.hologres.client.impl.collector.ActionCollector;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Partition;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 执行资源池，维护请求和工作线程.
 * 请求： clientMap，所有HoloCLient和ActionColllector的映射
 * 工作线程：
 * 1 workers，每个worker维护一个jdbc连接，处理Action
 * 2 commitTimer(commitJob), 定时调用所有collector的tryCommit方法
 * 3 readActionWatcher， 监控读请求队列，来了就第一时间丢给worker
 */
public class ExecutionPool implements Closeable {

	public static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPool.class);

	static final Map<String, ExecutionPool> POOL_MAP = new ConcurrentHashMap<>();

	//----------------------各类工作线程--------------------------------------
	//监控读队列，一来数据就丢給worker
	private ActionWatcher readActionWatcher;
	/**
	 * 后台线程.
	 * 1 触发所有的collector去检查是否满足writeMaxIntervalMs条件，及时像后端提交任务
	 * 2 尝试刷新满足metaAutoRefreshFactor的表的tableSchema
	 */
	private Runnable backgroundJob;
	//处理所有Action的runnable，push模式，由其他线程主动把任务塞給worker
	private Worker[] workers;
	//处理只跑一次的worker
	private Map<String, OneshotWorker> oneShotWorkers;
	private Semaphore writeSemaphore;
	private Semaphore readSemaphore;

	//挂到shutdownHook上，免得用户忘记关闭了
	Thread shutdownHandler = null;

	//----------------------------------------------------------------------

	private String name;
	private final Map<HoloClient, ActionCollector> clientMap;

	private AtomicBoolean started; //executionPool整体是否在运行中 ，false以后submit将抛异常
	private AtomicBoolean workerStated; //worker是否在运行中，false以后worker.offer将抛异常

	private Tuple<String, HoloClientException> fatalException = null;
	final ArrayBlockingQueue<Get> queue;
	final ByteSizeCache byteSizeCache;

	private final MetaStore metaStore;

	ExecutorService workerExecutorService;
	ThreadFactory workerThreadFactory;

	ExecutorService backgroundExecutorService;
	ThreadFactory backgroundThreadFactory;

	ThreadFactory ontShotWorkerThreadFactory;

	int writeThreadSize;
	int readThreadSize;
	final boolean refreshBeforeGetTableSchema;
	final int refreshMetaTimeout;
	final boolean enableShutdownHook;
	final HoloConfig config;
	final boolean isShadingEnv;
	final boolean isFixedPool; //当前ExecutionPool是不是fixed fe execution Pool

	public static ExecutionPool buildOrGet(String name, HoloConfig config) {
		return buildOrGet(name, config, true);
	}

	public static ExecutionPool buildOrGet(String name, HoloConfig config, boolean isShadingEnv) {
		return buildOrGet(name, config, isShadingEnv, false);
	}

	public static ExecutionPool buildOrGet(String name, HoloConfig config, boolean isShadingEnv, boolean isFixedPool) {
		// 启用和未启用fixed fe的HoloConfig彻底区分开,复用仅在isUseFixedFe参数相同时发生
		ExecutionPool pool = POOL_MAP.computeIfAbsent(name + config.isUseFixedFe(), n -> new ExecutionPool(n, config, isShadingEnv, isFixedPool));
		pool.resizeWorkers(config);
		return pool;
	}

	public ExecutionPool(String name, HoloConfig config, boolean isShadingEnv, boolean isFixedPool) {
		this.name = name;
		this.config = config;
		this.isShadingEnv = isShadingEnv;
		this.isFixedPool = isFixedPool;
		workerThreadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(ExecutionPool.this.name + "-worker");
				t.setDaemon(false);
				return t;
			}
		};
		backgroundThreadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(ExecutionPool.this.name + "-background");
				t.setDaemon(false);
				return t;
			}
		};
		ontShotWorkerThreadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(ExecutionPool.this.name + "-oneshot-worker");
				t.setDaemon(false);
				return t;
			}
		};

		this.refreshBeforeGetTableSchema = config.isRefreshMetaBeforeGetTableSchema();
		this.refreshMetaTimeout = config.getRefreshMetaTimeout();
		this.enableShutdownHook = config.isEnableShutdownHook();
		this.queue = new ArrayBlockingQueue<>(config.getReadBatchQueueSize());
		readActionWatcher = new ActionWatcher(config.getReadBatchSize());
		this.readThreadSize = 0;
		this.writeThreadSize = 0;
		workers = new Worker[0];
		oneShotWorkers = new HashMap<>();
		started = new AtomicBoolean(false);
		workerStated = new AtomicBoolean(false);
		resizeWorkers(config);
		clientMap = new ConcurrentHashMap<>();
		byteSizeCache = new ByteSizeCache(config.getWriteBatchTotalByteSize());
		backgroundJob = new BackgroundJob(config);
		this.metaStore = new MetaStore(config.getMetaCacheTTL());
	}

	private synchronized void start() throws HoloClientException {
		if (started.compareAndSet(false, true)) {
			LOGGER.info("HoloClient ExecutionPool[{}] start", name);
			closeStack = null;
			workerStated.set(true);
			workerExecutorService = new ThreadPoolExecutor(workers.length, workers.length, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1), workerThreadFactory, new ThreadPoolExecutor.AbortPolicy());
			backgroundExecutorService = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1), backgroundThreadFactory, new ThreadPoolExecutor.AbortPolicy());
			for (int i = 0; i < workers.length; ++i) {
				workerExecutorService.execute(workers[i]);
			}
			if (this.enableShutdownHook) {
				shutdownHandler = new Thread(() -> close());
				Runtime.getRuntime().addShutdownHook(shutdownHandler);
			}
			backgroundExecutorService.execute(backgroundJob);
			backgroundExecutorService.execute(readActionWatcher);
			this.writeSemaphore = new Semaphore(this.writeThreadSize);
			this.readSemaphore = new Semaphore(this.readThreadSize);
		}
	}

	Tuple<String, HoloClientException> closeStack = null;

	public synchronized Tuple<String, HoloClientException> getCloseReasonStack() {
		return closeStack;
	}

	public boolean isFixedPool() {
		return isFixedPool;
	}

	@Override
	public synchronized void close() {
		if (started.compareAndSet(true, false)) {
			closeStack = new Tuple<>(LocalDateTime.now().toString(),
				new HoloClientException(ExceptionCode.ALREADY_CLOSE, "close caused by"));
			if (clientMap.size() > 0) {
				LOGGER.warn("HoloClient ExecutionPool[{}] close, current client size {}", name, clientMap.size());
			} else {
				LOGGER.info("HoloClient ExecutionPool[{}] close", name);
			}
			if (shutdownHandler != null) {
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHandler);
				} catch (Exception e) {
					LOGGER.warn("", e);
				}
			}
			try {
				backgroundExecutorService.shutdownNow();
				while (!backgroundExecutorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
					LOGGER.info("wait background executorService termination[{}]", name);
				}
				backgroundExecutorService = null;
			} catch (InterruptedException ignore) {
			}
			workerStated.set(false);
			for (Worker worker : workers) {
				try {
					worker.offer(null);
				} catch (HoloClientException ignore) {

				}
			}
			try {
				workerExecutorService.shutdown();
				while (!workerExecutorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
					LOGGER.info("wait worker executorService termination[{}]", name);
				}
				workerExecutorService = null;
				backgroundExecutorService = null;
			} catch (InterruptedException ignore) {
			}
			this.writeSemaphore = null;
			this.readSemaphore = null;

			POOL_MAP.remove(name);
		}
	}

	public Partition getOrSubmitPartition(TableName tableName, String partValue, boolean isStr) throws HoloClientException {
		return getOrSubmitPartition(tableName, partValue, isStr, false);
	}

	public Partition getOrSubmitPartition(TableName tableName, String partValue, boolean isStr, boolean createIfNotExists) throws HoloClientException {
		try {
			return metaStore.partitionCache.get(tableName).get(partValue, v -> {
				SqlAction<Partition> partitionAction = new SqlAction<>((conn) -> {
					String value = partValue;
					if (refreshMetaTimeout > 0) {
						ConnectionUtil.refreshMeta(conn, refreshMetaTimeout);
					}
					Partition internalPartition = ConnectionUtil.getPartition(conn, tableName.getSchemaName(), tableName.getTableName(), value, isStr);
					if (internalPartition == null) {
						if (!createIfNotExists) {
							return null;
						} else {
							try {
								internalPartition = ConnectionUtil.retryCreatePartitionChildTable(conn, tableName.getSchemaName(), tableName.getTableName(), value, isStr);
							} catch (SQLException e) {
								internalPartition = ConnectionUtil.getPartition(conn, tableName.getSchemaName(), tableName.getTableName(), value, isStr);
								if (internalPartition == null) {
									throw new SQLException("create partition fail and not found the partition", e);
								}
							}
							if (internalPartition != null) {
								return internalPartition;
							} else {
								throw new SQLException("after create, partition child table is still not exists, tableName:" + tableName.getFullName() + ",partitionValue:" + value);
							}
						}
					} else {
						return internalPartition;
					}
				});
				try {
					while (!submit(partitionAction)) {

					}
					return partitionAction.getResult();
				} catch (HoloClientException e) {
					throw new SQLException(e);
				}
			});
		} catch (SQLException e) {
			throw HoloClientException.fromSqlException(e);
		} catch (Exception e) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "getOrSubmitPartition fail. tableName=" + tableName.getFullName() + ", partValue=" + partValue, e);
		}
	}

	public TableSchema getOrSubmitTableSchema(TableName tableName, boolean noCache) throws HoloClientException {

		try {
			return metaStore.tableCache.get(tableName, (tn) -> {
				try {
					MetaAction metaAction = new MetaAction(tableName);
					while (!submit(metaAction)) {
					}
					return metaAction.getResult();
				} catch (HoloClientException e) {
					throw new SQLException(e);
				}
			}, noCache ? Cache.MODE_NO_CACHE : Cache.MODE_LOCAL_THEN_REMOTE);
		} catch (SQLException e) {
			throw HoloClientException.fromSqlException(e);
		}
	}


	/**
	 * oneshot是靠started自己去控制的，如果started一直不false，也就不会结束.
	 *
	 * @param started
	 * @param index
	 * @param action
	 * @return
	 * @throws HoloClientException
	 */
	public Thread submitOneShotAction(AtomicBoolean started, String index, AbstractAction action) throws HoloClientException {
		OneshotWorker worker = new OneshotWorker(config, started, index, isShadingEnv, isFixedPool);
		boolean ret = worker.offer(action);
		if (!ret) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "submitOneShotAction fail");
		}
		Thread thread = ontShotWorkerThreadFactory.newThread(worker);
		oneShotWorkers.put(index, worker);
		thread.start();
		return thread;
	}

	/**
	 * @param action action
	 * @return 提交成功返回true；所有worker都忙时返回false
	 */
	public boolean submit(AbstractAction action) throws HoloClientException {
		if (!started.get()) {
			throw new HoloClientException(ExceptionCode.ALREADY_CLOSE, "submit fail");
		}
		Semaphore semaphore = null;
		int start = -1;
		int end = -1;
		if (action instanceof PutAction) {
			semaphore = writeSemaphore;
			start = 0;
			end = Math.min(writeThreadSize, workers.length);
		} else if (action instanceof GetAction || action instanceof ScanAction) {
			semaphore = readSemaphore;
			start = Math.max(0, workers.length - readThreadSize);
			end = workers.length;
		} else {
			start = 0;
			end = workers.length;
		}

		//如果有信号量，尝试获取信号量，否则返回submit失败
		if (semaphore != null) {
			try {
				boolean acquire = semaphore.tryAcquire(2000L, TimeUnit.MILLISECONDS);
				if (!acquire) {
					return false;
				}
			} catch (InterruptedException e) {
				throw new HoloClientException(ExceptionCode.INTERRUPTED, "");
			}
			action.setSemaphore(semaphore);
		}
		//尝试提交
		for (int i = start; i < end; ++i) {
			Worker worker = workers[i];
			if (worker.offer(action)) {
				return true;
			}
		}
		//提交失败则释放,提交成功Worker会负责释放
		if (semaphore != null) {
			semaphore.release();
		}
		return false;
	}

	public MetaStore getMetaStore() {
		return metaStore;
	}

	public int getWorkerCount() {
		return workers.length;
	}

	public synchronized ActionCollector register(HoloClient client, HoloConfig config) throws HoloClientException {
		boolean needStart = false;
		ActionCollector collector = null;
		synchronized (clientMap) {
			boolean empty = clientMap.isEmpty();
			collector = clientMap.get(client);
			if (collector == null) {
				LOGGER.info("register client {}, client size {}->{}", client, clientMap.size(), clientMap.size() + 1);
				collector = new ActionCollector(config, this, queue);
				clientMap.put(client, collector);
				if (empty) {
					needStart = true;
				}
			}
		}
		if (needStart) {
			start();
		}
		return collector;
	}

	public synchronized boolean isRegister(HoloClient client) {
		synchronized (clientMap) {
			return clientMap.containsKey(client);
		}
	}

	public synchronized void unregister(HoloClient client) {
		boolean needClose = false;
		synchronized (clientMap) {
			int oldSize = clientMap.size();
			if (oldSize > 0) {
				clientMap.remove(client);
				int newSize = clientMap.size();
				LOGGER.info("unregister client {}, client size {}->{}", client, oldSize, newSize);
				if (newSize == 0) {
					needClose = true;
				}
			}
		}
		if (needClose) {
			close();
		}
	}

	// VisibleForTesting
	public synchronized int getClientMapSize() {
		synchronized (clientMap) {
            return clientMap.size();
        }
	}

	private synchronized void resizeWorkers(HoloConfig config) {
		int readThreadSize = config.getReadThreadSize();
		int writeThreadSize = config.getWriteThreadSize();
		// Fe模式: workerSize取读并发和写并发的最大值，worker会公用
		// FixedFe模式: 分为fixedPool（workerSize取读并发和写并发的最大值，worker会公用）和fePool（workerSize设置为connectionSizeWhenUseFixedFe, 用于sql、meta等其他action）
		int workerSize;
		if (config.isUseFixedFe() && !isFixedPool) {
			workerSize = config.getConnectionSizeWhenUseFixedFe();
		} else {
			workerSize = Math.max(readThreadSize, writeThreadSize);
		}
		// 只往大了resize
		workerSize = Math.max(workerSize, workers.length);
		if (workerStated.get()) {
			((ThreadPoolExecutor) workerExecutorService).setMaximumPoolSize(workerSize);
			((ThreadPoolExecutor) workerExecutorService).setCorePoolSize(workerSize);
			writeSemaphore.release(Math.max(writeThreadSize - this.writeThreadSize, 0));
			readSemaphore.release(Math.max(readThreadSize - this.readThreadSize, 0));
		}
		Worker[] newWorkers = new Worker[workerSize];
		System.arraycopy(workers, 0, newWorkers, 0, workers.length);
		for (int i = workers.length; i < workerSize; ++i) {
			if (isFixedPool) {
				newWorkers[i] = new Worker(config, workerStated, String.valueOf(i), isShadingEnv, true);
			} else {
				newWorkers[i] = new Worker(config, workerStated, String.valueOf(i), isShadingEnv);
			}
			if (workerStated.get()) {
				workerExecutorService.execute(newWorkers[i]);
			}
		}
		this.writeThreadSize = writeThreadSize;
		this.readThreadSize = readThreadSize;
		workers = newWorkers;
	}

	public boolean isRunning() {
		return started.get();
	}

	public void tryThrowException() throws HoloClientException {
		if (fatalException != null) {
			throw new HoloClientException(fatalException.r.getCode(), String.format(
					"An exception occurred at %s and data may be lost, please restart holo-client and recover from the "
							+ "last checkpoint",
					fatalException.l), fatalException.r);
		}
	}

	/**
	 * 整个ExecutionPool的内存估算.
	 */
	class ByteSizeCache {
		final long maxByteSize;
		long value = 0L;
		AtomicLong last = new AtomicLong(System.nanoTime());

		public ByteSizeCache(long maxByteSize) {
			this.maxByteSize = maxByteSize;
		}

		long getAvailableByteSize() {
			return maxByteSize - getByteSize();
		}

		long getByteSize() {
			long nano = last.get();
			long current = System.nanoTime();
			if (current - nano > 2 * 1000 * 1000000L && last.compareAndSet(nano, current)) {
				long sum = clientMap.values().stream().collect(Collectors.summingLong(ActionCollector::getByteSize));
				value = sum;
			}
			return value;
		}
	}

	public long getAvailableByteSize() {
		return byteSizeCache.getAvailableByteSize();
	}

	class BackgroundJob implements Runnable {

		long tableSchemaRemainLife;
		long forceFlushInterval;
		AtomicInteger pendingRefreshTableSchemaActionCount;

		public BackgroundJob(HoloConfig config) {
			forceFlushInterval = config.getForceFlushInterval();
			tableSchemaRemainLife = config.getMetaCacheTTL() / config.getMetaAutoRefreshFactor();
			pendingRefreshTableSchemaActionCount = new AtomicInteger(0);
		}

		long lastForceFlushMs = -1L;

		private void triggerTryFlush() {
			synchronized (clientMap) {
				boolean force = false;

				if (forceFlushInterval > 0L) {
					long current = System.currentTimeMillis();
					if (current - lastForceFlushMs > forceFlushInterval) {
						force = true;
						lastForceFlushMs = current;
					}
				}
				for (ActionCollector collector : clientMap.values()) {
					try {
						if (force) {
							collector.flush(true);
						} else {
							collector.tryFlush();
						}
					} catch (HoloClientException e) {
						fatalException = new Tuple<>(LocalDateTime.now().toString(), e);
						break;
					}
				}
			}
		}

		private void refreshTableSchema() {
			//避免getTableSchema返回太慢的时候，同个表重复刷新TableSchema
			if (pendingRefreshTableSchemaActionCount.get() == 0) {
				// 万一出现非预期的异常也不会导致线程结束工作
				try {
					metaStore.tableCache.filterKeys(tableSchemaRemainLife).forEach(tableNameWithState -> {
						Cache.ItemState state = tableNameWithState.l;
						TableName tableName = tableNameWithState.r;
						switch (state) {
							case EXPIRE:
								LOGGER.info("remove expire tableSchema for {}", tableName);
								metaStore.tableCache.remove(tableName);
								break;
							case NEED_REFRESH:
								try {
									getOrSubmitTableSchema(tableName, true);
									MetaAction metaAction = new MetaAction(tableName);
									if (submit(metaAction)) {
										LOGGER.info("refresh tableSchema for {}, because remain lifetime < {} ms", tableName, tableSchemaRemainLife);
										pendingRefreshTableSchemaActionCount.incrementAndGet();
										metaAction.getFuture().whenCompleteAsync((tableSchema, exception) -> {
											pendingRefreshTableSchemaActionCount.decrementAndGet();
											if (exception != null) {
												LOGGER.warn("refreshTableSchema fail", exception);
												if (exception.getMessage() != null && exception.getMessage().contains("can not found table")) {
													metaStore.tableCache.remove(tableName);
												}
											} else {
												metaStore.tableCache.put(tableName, tableSchema);
											}
										});
									}
								} catch (Exception e) {
									LOGGER.warn("refreshTableSchema fail", e);
								}
								break;
							default:
								LOGGER.error("undefine item state {}", state);
						}

					});
				} catch (Throwable e) {
					LOGGER.warn("refreshTableSchema unexpected fail", e);
				}
			}
		}

		@Override
		public void run() {
			while (started.get()) {
				triggerTryFlush();
				refreshTableSchema();
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ignore) {
					break;
				}
			}
		}

		@Override
		public String toString() {
			return "CommitJob";
		}
	}

	/**
	 * 监控所有的查询Action，提交給Worker.
	 * 读和写机制不一样。
	 * 写是攒批满足条件后提交給Worker；
	 * 读需要一个专门的线程把读队列里的请求第一时间取出并提交給Worker来保证latency。
	 */
	class ActionWatcher implements Runnable {
		private int batchSize;

		public ActionWatcher(int batchSize) {
			this.batchSize = batchSize;
		}

		@Override
		public void run() {
			List<Get> recordList = new ArrayList<>(batchSize);
			while (started.get()) {
				try {
					recordList.clear();
					Get firstGet = queue.poll(2, TimeUnit.SECONDS);
					if (firstGet != null) {
						recordList.add(firstGet);
						queue.drainTo(recordList, batchSize - 1);
						Map<Tuple<TableSchema, TableName>, List<Get>> getsByTable = new HashMap<>();
						for (Get get : recordList) {
							if (config.getReadTimeoutMilliseconds() > 0) {
								long waitingTime = (System.nanoTime() - get.getStartTime()) / 1000000L;
								if (waitingTime - config.getReadTimeoutMilliseconds() > 0) {
									get.getFuture().completeExceptionally(new HoloClientException(ExceptionCode.TIMEOUT,
											String.format(
													"get waiting timeout before submit to holo, it cost %s ms greater than %s ms set in the config, "
															+ "maybe the holo-client is too busy and increasing readThreadSize can resolve it",
													waitingTime, config.getReadTimeoutMilliseconds())));
								}
							}
							List<Get> list = getsByTable.computeIfAbsent(new Tuple<>(get.getRecord().getSchema(), get.getRecord().getTableName()), (s) -> new ArrayList<Get>());
							list.add(get);
						}
						for (Map.Entry<Tuple<TableSchema, TableName>, List<Get>> entry : getsByTable.entrySet()) {
							GetAction getAction = new GetAction(entry.getValue());
							while (!submit(getAction)) {
							}
						}
					}
				} catch (InterruptedException e) {
					break;
				} catch (Exception e) {
					for (Get get : recordList) {
						if (!get.getFuture().isDone()) {
							get.getFuture().completeExceptionally(e);
						}
					}
				}
			}
		}

		@Override
		public String toString() {
			return "ActionWatcher{" +
					"batchSize=" + batchSize +
					'}';
		}
	}
}
