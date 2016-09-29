package com.wlu.orm.hbase.schema.value;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class SerializedValue implements Value {

	private Object obj;

	public SerializedValue(Object instance) {
		this.obj = instance;
	}
	
	public String getSerializedValue() {
		return obj.toString();
	}

	public Object getObjectValue() {
		return obj;
	}
	
	@Override
	public byte[] toBytes() {
		ObjectOutput out = null;
		try(ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
		  out = new ObjectOutputStream(bos);   
		  out.writeObject(obj);
		  out.flush();
		 return bos.toByteArray();
		}catch (Exception e) {
			throw new RuntimeException("Error serializing to bytes");
		}
	}

	@Override
	public String getType() {
		return "JsonNodeValue";
	}
}
