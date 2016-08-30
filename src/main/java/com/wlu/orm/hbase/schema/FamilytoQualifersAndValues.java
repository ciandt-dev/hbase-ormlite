package com.wlu.orm.hbase.schema;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.wlu.orm.hbase.schema.value.StringValue;
import com.wlu.orm.hbase.schema.value.Value;

/**
 * like a map from a family to a list of qualifers
 * 
 * @author wlu
 * 
 */
public class FamilytoQualifersAndValues {
	private static Log LOG = LogFactory.getLog(FamilytoQualifersAndValues.class);
	
	private byte[] family;
	private Map<byte[], Value> qualifierValue = new HashMap<byte[], Value>();

	public FamilytoQualifersAndValues(byte[] family,
			Map<byte[], Value> qualiferValue) {
		this.family = family;
		this.qualifierValue = qualiferValue;
	}

	public FamilytoQualifersAndValues() {
	}

	// add family:qualifier->value to a Put
	public Put addToPut(Put put) {
		if (put == null) {
			return null;
		}
		for (byte[] qualifier : qualifierValue.keySet()) {
			try{
				put.addColumn(family, qualifier, qualifierValue.get(qualifier).toBytes());
			} catch (Exception e){
				LOG.error("Error trying to addToPut info to Put Object", e);
			}
		}
		return put;
	}
	
	public byte[] getFamily() {
		return family;
	}

	public void setFamily(byte[] family) {
		this.family = family;
	}

	public Map<byte[], Value> getQualiferValue() {
		return qualifierValue;
	}

	public void setQualiferValue(Map<byte[], Value> qualiferValue) {
		this.qualifierValue = qualiferValue;
	}

	/**
	 * add qualifier and value to the map
	 * 
	 * @param qualifier
	 * @param value
	 */
	public void add(byte[] qualifier, Value value) {
		qualifierValue.put(qualifier, value);
	}

	public void add(Map<byte[], Value> qualifierValues) {
		if (qualifierValues == null) {
			return;
		}
		qualifierValue.putAll(qualifierValues);
	}

}
