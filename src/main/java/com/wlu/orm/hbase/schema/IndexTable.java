package com.wlu.orm.hbase.schema;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;

import com.wlu.orm.hbase.schema.value.Value;
import com.wlu.orm.hbase.schema.value.ValueFactory;

/**
 * @author Felipe Amaral - famaral@ciandt.com
 *
 * <br/>created at 08/26/2016
 *
 */
public class IndexTable {

	private static Log LOG = LogFactory.getLog(IndexTable.class);

	private static byte[] indexTableFamilyQualifier = DataMapperFactory.INDEX_ROW_KEY.getBytes();

	private List<Put> putList;

	private String tableName;

	public IndexTable(String tablename) {
		tableName = tablename + DataMapperFactory.INDEX_TABLE_SUFFIX;
	}

	public void add(Field field, Value rowkey, FamilytoQualifersAndValues familytoQualifersAndValues) {
		LOG.info("Adding new Field on Index Table. Field name:" + field.getName());
		byte[] value = rowkey.toBytes();
		Map<byte[], Value> qualifierValue = familytoQualifersAndValues.getQualiferValue();
		for (byte[] qualifier : qualifierValue.keySet()) {
			byte[] rowKey = generateRowKey(field, qualifierValue.get(qualifier)); 
			addPut(new Put(rowKey).addColumn(indexTableFamilyQualifier, indexTableFamilyQualifier, value));
		}
	}

	private byte[] generateRowKey(Field field, Value value) {
		Object obj = ValueFactory.getValue(value);
		return (field.getName()+":"+obj.toString()).getBytes();
	}

	public void addPut(Put put){
		if(putList == null){
			putList = new ArrayList<Put>();
		}
		putList.add(put);
	}

	public List<Put> getPut() {
		return putList;
	}

	public String getTableName() {
		return tableName;
	}
}
