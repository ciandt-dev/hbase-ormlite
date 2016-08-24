package com.wlu.orm.hbase.schema.value;

import org.apache.hadoop.hbase.util.Bytes;

public class BooleanValue implements Value {

	private boolean booleanValue;

	public BooleanValue(boolean intValue) {
		super();
		this.booleanValue = intValue;
	}

	public Boolean getBooleanValue() {
		return booleanValue;
	}

	public void setBooleanValue(boolean intValue) {
		this.booleanValue = intValue;
	}

	@Override
	public byte[] toBytes() {
		return Bytes.toBytes(booleanValue);
	}

	@Override
	public String getType() {
		return "booleanValue";
	}

}
