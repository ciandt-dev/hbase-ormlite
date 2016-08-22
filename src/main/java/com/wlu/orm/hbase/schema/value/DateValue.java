package com.wlu.orm.hbase.schema.value;

import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public class DateValue implements Value {

	private Date dateValue;

	public DateValue(Date intValue) {
		super();
		this.dateValue = intValue;
	}

	public Date getDateValue() {
		return dateValue;
	}

	public void setDateValue(Date intValue) {
		this.dateValue = intValue;
	}

	@Override
	public byte[] toBytes() {
		return Bytes.toBytes(dateValue.getTime());
	}

	@Override
	public String getType() {
		return "dateValue";
	}

}
