package com.alibaba.ververica.connectors.hologres.jdbc.source.lookup;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.common.dim.DimJoinFetcher;
import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import com.alibaba.ververica.connectors.hologres.jdbc.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.jdbc.serde.HologresRowConverter;
import com.alibaba.ververica.connectors.hologres.jdbc.util.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** An abstract hologres sync or async row fetcher class. */
public abstract class AbstractHologresLookupFunction extends DimJoinFetcher
		implements ResultTypeQueryable<RowData> {

	private static final long serialVersionUID = 3341593566682551111L;
	private static final Logger LOG = LoggerFactory.getLogger(AbstractHologresLookupFunction.class);
	protected final HologresParams param;
	protected final CacheStrategy cacheStrategy;
	protected final int fieldLength;
	protected final String[] fieldNames;
	protected final DataType[] fieldTypes;
	protected final List<Integer> sourceKeys;
	protected final List<Integer> targetKeys;
	protected final LogicalType[] logicalTypes;
	protected final LogicalType[] indexColumnTypes;
	protected final List<String> keyFilters;
	protected TypeInformation<RowData> rowTypeInfo;
	private org.apache.flink.table.data.RowData.FieldGetter[] fieldGetters;
	protected transient HologresRowConverter converter;
	protected transient HoloClient holoClient;
	protected transient org.postgresql.model.TableSchema hologresSchema;

	public AbstractHologresLookupFunction(
			String sqlTableName,
			TableSchema tableSchema,
			String[] index,
			HologresParams param,
			CacheStrategy cacheStrategy) {
		super(sqlTableName, index, cacheStrategy);
		this.param = param;
		this.cacheStrategy = cacheStrategy;

		this.rowTypeInfo = FlinkUtil.getRowTypeInfo(tableSchema);
		this.fieldLength = tableSchema.getFieldCount();
		this.fieldTypes = tableSchema.getFieldDataTypes();
		this.fieldNames = tableSchema.getFieldNames();

		this.sourceKeys = new ArrayList<>();
		this.targetKeys = new ArrayList<>();
		this.keyFilters = new ArrayList<>();
		this.fieldGetters = new org.apache.flink.table.data.RowData.FieldGetter[index.length];
		this.indexColumnTypes = new LogicalType[index.length];

		this.logicalTypes = new LogicalType[fieldLength];
		for (int idx = 0; idx < fieldLength; idx++) {
			String fieldName = tableSchema.getFieldNames()[idx];
			logicalTypes[idx] = tableSchema.getFieldDataType(fieldName).get().getLogicalType();
		}

		for (int i = 0; i < index.length; i++) {
			this.sourceKeys.add(i);
			int targetIdx = -1;
			for (int j = 0; j < fieldNames.length; j++) {
				if (fieldNames[j].equals(index[i])) {
					targetIdx = j;
					break;
				}
			}
			this.targetKeys.add(targetIdx);
			this.indexColumnTypes[i] = fieldTypes[targetIdx].getLogicalType();
			this.keyFilters.add(this.fieldNames[targetIdx]);
			this.fieldGetters[i] = RowData.createFieldGetter(
							fieldTypes[targetKeys.get(i)].getLogicalType(), sourceKeys.get(i));
		}
	}

	@Override
	public void openConnection(Configuration parameters) {
		LOG.info("Initiating connection to database [{}] / table[{}]", param.getJdbcOptions().getDatabase(), param.getTable());
		HoloConfig holoConfig = new HoloConfig();
		JDBCOptions jdbcOptions = param.getJdbcOptions();
		holoConfig.setJdbcUrl(jdbcOptions.getDbUrl());
		holoConfig.setUsername(jdbcOptions.getUsername());
		holoConfig.setPassword(jdbcOptions.getPassword());
		try {
			holoClient = new HoloClient(holoConfig);
			hologresSchema = holoClient.getTableSchema(jdbcOptions.getTable());
			converter = new HologresRowConverter(fieldNames, logicalTypes, param);
			converter.setHologresColumnSchema(hologresSchema);
		} catch (HoloClientException e) {
			throw new RuntimeException(e);
		}
		LOG.info("Successfully initiated connection to database [{}] / table[{}]", param.getJdbcOptions().getDatabase(), param.getTable());
	}

	@Override
	public void closeConnection() {
		LOG.info("Closing HologresLookUpFunction");
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return this.rowTypeInfo;
	}

	protected Object getSourceKey(org.apache.flink.table.data.RowData source) {
		return getKey(source, sourceKeys, indexColumnTypes);
	}

	protected Get createGetRequest(org.apache.flink.table.data.RowData in) {
		Get.Builder builder = Get.newBuilder(hologresSchema);
		builder.withSelectedColumns(fieldNames);
		for (int i = 0; i < keyFilters.size(); ++i) {
			Object value = fieldGetters[i].getFieldOrNull(in);
			builder.setPrimaryKey(keyFilters.get(i), value);
		}
		return builder.build();
	}

	/**
	 * Parse the Hologres Record into BaseRow.
	 */
	protected org.apache.flink.table.data.RowData parseToRow(Record record) throws SQLException {
		// parse to Row
		Preconditions.checkArgument(fieldLength <= record.getLength(),
				"fieldLength = " + fieldLength + ", record size = " + record.getLength());
		return converter.convertFromHologresRecord(record);
	}
}
