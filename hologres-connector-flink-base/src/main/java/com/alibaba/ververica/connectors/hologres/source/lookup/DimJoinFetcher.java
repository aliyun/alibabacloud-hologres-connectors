package com.alibaba.ververica.connectors.hologres.source.lookup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.alibaba.ververica.connectors.common.dim.cache.AllCache;
import com.alibaba.ververica.connectors.common.dim.cache.Cache;
import com.alibaba.ververica.connectors.common.dim.cache.CacheFactory;
import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.common.dim.reload.CacheAllReloadConf;
import com.alibaba.ververica.connectors.common.dim.reload.SerializableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** DimJoinFetcher. */
public abstract class DimJoinFetcher<V> extends AbstractRichFunction {
    private static final Logger LOG =
            LoggerFactory.getLogger(
                    com.alibaba.ververica.connectors.common.dim.DimJoinFetcher.class);
    protected final String sqlTableName;
    protected final RowType dimRowType;
    protected final String[] lookupKeys;
    protected final CacheStrategy cacheStrategy;
    protected SerializableRunnable cacheReloadRunner;
    protected CacheAllReloadConf reloadConf;
    protected transient CacheFactory<Object, V> cacheFactory;
    protected transient Cache<Object, V> cache;
    protected transient ScheduledExecutorService reloadExecutor;
    protected transient AllCache<Object, V> allCacheHandler;
    private transient String cacheId;
    private List<Integer> sourceKeys;
    private List<Integer> targetKeys;
    private LogicalType[] keyTypes;
    private transient Projection<RowData, BinaryRowData> srcKeyProjection;
    private transient Projection<RowData, BinaryRowData> cacheKeyProjection;
    private RowDataSerializer cacheKeySer;
    protected RowDataSerializer cacheRowSer;

    protected DimJoinFetcher(
            String sqlTableName,
            RowType dimRowType,
            String[] lookupKeys,
            CacheStrategy cacheStrategy) {
        Preconditions.checkArgument(null != sqlTableName, "sqlTableName cannot be null!");
        Preconditions.checkArgument(null != lookupKeys, "lookupKeys cannot be null!");
        Preconditions.checkArgument(null != dimRowType, "dimRowType cannot be null!");
        Preconditions.checkArgument(null != cacheStrategy, "cacheStrategy cannot be null!");
        this.dimRowType = dimRowType;
        this.lookupKeys = lookupKeys;
        this.sqlTableName = sqlTableName;
        this.cacheStrategy = cacheStrategy;
        this.sourceKeys = new ArrayList();
        this.targetKeys = new ArrayList();
        this.cacheRowSer = new RowDataSerializer(dimRowType);
        this.keyTypes = new LogicalType[lookupKeys.length];
        String[] fieldNames = (String[]) dimRowType.getFieldNames().toArray(new String[0]);

        for (int i = 0; i < lookupKeys.length; ++i) {
            this.sourceKeys.add(i);
            int targetIdx = this.getColumnIndex(lookupKeys[i], fieldNames);
            if (targetIdx < 0) {
                throw new TableException("Column: " + lookupKeys[i] + " doesn't exists.");
            }

            this.targetKeys.add(targetIdx);
            this.keyTypes[i] = dimRowType.getTypeAt(targetIdx);
        }

        this.cacheKeySer = new RowDataSerializer(this.keyTypes);
    }

    public void setAllCacheReloadRunner(
            SerializableRunnable cacheReloadRunner, CacheAllReloadConf reloadConf) {
        if (this.cacheStrategy.isAllCache()) {
            Objects.requireNonNull(cacheReloadRunner);
            Objects.requireNonNull(reloadConf);
            Objects.requireNonNull(reloadConf.timeRangeBlackList);
            this.cacheReloadRunner = cacheReloadRunner;
            this.reloadConf = reloadConf;
        }
    }

    public abstract void openConnection(Configuration var1);

    public abstract void closeConnection();

    public abstract boolean hasPrimaryKey();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.openConnection(parameters);

        this.cacheId = this.sqlTableName + ": " + Arrays.toString(this.lookupKeys);

        String cacheName = this.hasPrimaryKey() ? "one2oneCahce" : "one2manyCache";
        LOG.info("table " + this.sqlTableName + " preparing " + cacheName);
        this.cacheFactory = CacheFactory.getInstance();
        this.cache =
                this.cacheFactory.getCache(
                        this.cacheId,
                        this.cacheStrategy,
                        this.hasPrimaryKey(),
                        this.cacheKeySer,
                        this.cacheRowSer);
        LOG.info("table " + this.sqlTableName + ", strategy:" + this.cacheStrategy);
    }

    private ScheduledFuture<?> scheduleCacheLoaderRunner(String threadName) {
        this.reloadExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        (new ThreadFactoryBuilder())
                                .setNameFormat(threadName)
                                .setDaemon(true)
                                .build());
        return this.reloadExecutor.scheduleWithFixedDelay(
                new Thread(this.cacheReloadRunner),
                0L,
                this.reloadConf.ttlMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        if (this.cacheStrategy.isAllCache()
                && this.allCacheHandler.counter.decrementAndGet() == 0) {
            LOG.info("start to cancel reloading thread...");
            ScheduledFuture<?> future = this.allCacheHandler.getScheduledFuture();
            if (future != null) {
                LOG.info("start to cancel reloading thread for table: {}.", this.sqlTableName);
                future.cancel(true);
            }

            if (null != this.reloadExecutor && !this.reloadExecutor.isShutdown()) {
                this.reloadExecutor.shutdownNow();
                this.reloadExecutor = null;
            }

            this.allCacheHandler.isRegisteredTimer.compareAndSet(true, false);
            this.removeCache();
        }

        LOG.info("start to close connection...");
        this.closeConnection();
        if (!this.cacheStrategy.isAllCache()) {
            this.removeCache();
        }

        super.close();
    }

    private void removeCache() {
        LOG.info("start to release cache of table: {}...", this.sqlTableName);
        if (this.cacheFactory != null) {
            LOG.info("table " + this.sqlTableName + " cache removing...");
            this.cacheFactory.removeCache(this.cacheId);
            LOG.info("table " + this.sqlTableName + " cache removed");
        }
    }

    protected Object getSourceKey(RowData source) {
        return this.getKey(source, this.sourceKeys, this.keyTypes, this.srcKeyProjection, false);
    }

    protected Object prepareCacheKey(RowData target) {
        return this.getKey(target, this.targetKeys, this.keyTypes, this.cacheKeyProjection, true);
    }

    public Object getKey(
            RowData input,
            List<Integer> keys,
            LogicalType[] types,
            Projection<RowData, BinaryRowData> projection,
            boolean needCopy) {
        if (keys.size() == 1) {
            return this.safeGet(input, (Integer) keys.get(0), types[0]);
        } else if (projection != null) {
            return needCopy
                    ? ((BinaryRowData) projection.apply(input)).copy()
                    : projection.apply(input);
        } else {
            GenericRowData key = new GenericRowData(keys.size());

            for (int i = 0; i < keys.size(); ++i) {
                Object field = this.safeGet(input, (Integer) keys.get(i), types[i]);
                if (field == null) {
                    return null;
                }

                key.setField(i, field);
            }

            return key;
        }
    }

    public Object getKey(RowData input, List<Integer> keys, LogicalType[] types) {
        return this.getKey(input, keys, types, (Projection) null, false);
    }

    @VisibleForTesting
    public AllCache<Object, V> getAllCacheHandler() {
        return this.allCacheHandler;
    }

    protected Object safeGet(RowData inRow, int ordinal, LogicalType type) {
        if (inRow != null && !inRow.isNullAt(ordinal)) {
            FieldGetter fieldGetter = RowData.createFieldGetter(type, ordinal);
            return fieldGetter.getFieldOrNull(inRow);
        } else {
            return null;
        }
    }

    protected int getColumnIndex(String column, String[] columnNames) {
        for (int i = 0; i < columnNames.length; ++i) {
            if (column.equals(columnNames[i])) {
                return i;
            }
        }
        return -1;
    }
}
