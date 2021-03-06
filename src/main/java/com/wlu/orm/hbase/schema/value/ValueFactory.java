package com.wlu.orm.hbase.schema.value;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.Gson;

public class ValueFactory {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	static Class<Integer> INTEGER = Integer.class;
	static Class<Long> LONG = Long.class;
	static Class<Double> DOUBLE = Double.class;
	static Class<Float> FLOAT = Float.class;
	static Class<String> STRING = String.class;
	static Class<Date> DATE = Date.class;
	static Class<Boolean> BOOLEAN = Boolean.class;
	static Class<JsonNode> JSON_NODE = JsonNode.class;

	public static <T> Value createSerializeble(T instance){
		if (instance == null || instance.equals("null")) {
			return new NullValue();
		}
		if(instance instanceof Date){
			long time = ((Date)instance).getTime();
			return new SerializedValue(time);
		}
		return new SerializedValue(instance);
	}
	
	public static <T> Value create(T instance) {

		if (instance == null || instance.equals("null")) {
			return new NullValue();
		}

		Class<? extends Object> instanceClass = instance.getClass();
		if (instanceClass.equals(INTEGER)) {
			return new IntValue((Integer) instance);
		} else if (instanceClass.equals(LONG)) {
			return new LongValue((Long) instance);
		} else if (instanceClass.equals(DOUBLE)) {
			return new DoubleValue((Double) instance);
		} else if (instanceClass.equals(FLOAT)) {
			return new FloatValue((Float) instance);
		} else if (instanceClass.equals(STRING)) {
			return new StringValue((String) instance);
		} else if (instanceClass.equals(DATE)) {
			return new DateValue((Date)instance);
		} else if (instanceClass.equals(BOOLEAN)) {
			return new BooleanValue((Boolean)instance);
		} else {
			return new StringValue((String) instance.toString());
		}
	}

	/**
	 * Create a Object with type clazz from byte[]
	 * 
	 * @param clazz
	 * @param bytes
	 * @return
	 */
	public static Object createObject(Class<?> clazz, byte[] bytes) {
		
		if (bytes == null || bytes.length == 0) {
			return null;
		} else if (clazz.equals(int.class)) {
			return new Integer(Bytes.toInt(bytes));
		} else if (clazz.equals(long.class)) {
			return new Long(Bytes.toLong(bytes));
		} else if (clazz.equals(double.class)) {
			return new Double(Bytes.toDouble(bytes));
		} else if (clazz.equals(float.class)) {
			return new Float(Bytes.toFloat(bytes));
		} else if (clazz.equals(boolean.class)) {
			return new Boolean(Bytes.toBoolean(bytes));
		} else if (clazz.equals(INTEGER)) {
			return new Integer(Bytes.toInt(bytes));
		} else if (clazz.equals(LONG)) {
			return new Long(Bytes.toLong(bytes));
		} else if (clazz.equals(DOUBLE)) {
			return new Double(Bytes.toDouble(bytes));
		} else if (clazz.equals(FLOAT)) {
			return new Float(Bytes.toFloat(bytes));
		} else if (clazz.equals(STRING)) {
			return new String(Bytes.toString(bytes));
		} else if (clazz.equals(JSON_NODE)) {
			String result = null;
			try {
				result = OBJECT_MAPPER.writeValueAsString(OBJECT_MAPPER.readTree(Bytes.toString(bytes)));
			} catch (IOException e) {
				throw new RuntimeException("Error Trying to parse json");
			}
			return result;
		} else {
			return null;
		}
	}
	public static Object getValue(Value value) {
		Class<? extends Value> clazz = value.getClass();
		if (clazz.equals(IntValue.class)) {
			return ((IntValue)value).getIntValue();
		} else if (clazz.equals(LongValue.class)) {
			return ((LongValue)value).getLongValue();
		} else if (clazz.equals(DoubleValue.class)) {
			return ((DoubleValue)value).getDoubleValue();
		} else if (clazz.equals(FloatValue.class)) {
			return ((FloatValue)value).getFloatValue();
		} else if (clazz.equals(BooleanValue.class)) {
			return ((BooleanValue)value).getBooleanValue();
		} else if (clazz.equals(StringValue.class)) {
			return ((StringValue)value).getStringValue();
		} else if (clazz.equals(DateValue.class)) {
			return ((DateValue)value).getDateValue();
		} else if (clazz.equals(SerializedValue.class)) {
			return ((SerializedValue)value).getSerializedValue();
		} else {
			return null;
		}
	}
	
	/*
	 * used when directly create a Value
	 */
	public static Value TypeCreate(String value) {
		return new StringValue(value);
	}

	// TODO: add more

	class st {
		String name;
		String age;

		public st() {
			name = "jack";
			age = "234";
		}
	}

	public static void main(String args[]) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {
//		testFields tf = new testFields("abc", 1, 1.0f, 2d);
//		for (Field field : tf.getClass().getDeclaredFields()) {
//
//			Value v = Create(util.GetFromField(tf, field));
//			if (v == null) {
//				continue;
//			}
//			System.out.println(v.getType());
//		}

	}
}
