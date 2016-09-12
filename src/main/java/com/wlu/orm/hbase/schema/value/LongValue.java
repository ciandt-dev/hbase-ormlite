package com.wlu.orm.hbase.schema.value;

import org.apache.hadoop.hbase.util.Bytes;

public class LongValue implements Value {

	private Long longValue;

	public LongValue(Long longValue) {
		super();
		this.longValue = longValue;
	}

	public Long getLongValue() {
		return longValue;
	}

	public void setLongValue(Long longValue) {
		this.longValue = longValue;
	}

	@Override
	public byte[] toBytes() {
		return Bytes.toBytes(longValue);
	}

	@Override
	public String getType() {
		return "longValue";
	}

}
