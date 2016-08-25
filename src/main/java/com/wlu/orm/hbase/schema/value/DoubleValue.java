package com.wlu.orm.hbase.schema.value;

import org.apache.hadoop.hbase.util.Bytes;

public class DoubleValue implements Value {

	private double doubleValue;

	public DoubleValue(double doubleValue) {
		super();
		this.doubleValue = doubleValue;
	}

	

	public double getDoubleValue() {
		return doubleValue;
	}



	public void setDoubleValue(double intValue) {
		this.doubleValue = intValue;
	}



	@Override
	public byte[] toBytes() {
		return Bytes.toBytes(doubleValue);
	}

	@Override
	public String getType() {
		return "Double Value";
	}

}
