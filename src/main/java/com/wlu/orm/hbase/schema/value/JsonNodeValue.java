package com.wlu.orm.hbase.schema.value;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class JsonNodeValue implements Value {

	public static Gson gson;
	
	static {
		JsonSerializer<Date> ser = new JsonSerializer<Date>() {
			@Override
			public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext 
					context) {
				return src == null ? null : new JsonPrimitive(src.getTime());
			}
		};
		
		JsonDeserializer<Date> deser = new JsonDeserializer<Date>() {
			@Override
			public Date deserialize(JsonElement json, Type typeOfT,
					JsonDeserializationContext context) throws JsonParseException {
				return json == null ? null : new Date(json.getAsLong());
			}
		};
		gson = new GsonBuilder()
		   .registerTypeAdapter(Date.class, ser)
		   .registerTypeAdapter(Date.class, deser).create();	
	}
	
	private JsonNode jsonNode;
	
	private String valueAsString;

	public JsonNodeValue(Object instance) {
		setValueAsString(gson.toJson(instance));
	}

	public JsonNodeValue(){
	}
	
	public JsonNode getJsonNodeValue() {
		return jsonNode;
	}

	public void setJsonNodeValue(JsonNode jsonNodeValue) {
		this.jsonNode = jsonNodeValue;
	}

	@Override
	public byte[] toBytes() {
		String result = "";
		try {
			result = new ObjectMapper().writeValueAsString(jsonNode);
		} catch (IOException e) {
			throw new RuntimeException("Error trying to parse json");
		}
		return Bytes.toBytes(result);
	}

	@Override
	public String getType() {
		return "JsonNodeValue";
	}
	
	public String getValueAsString() {
		return valueAsString;
	}

	public void setValueAsString(String valueAsString) {
		this.valueAsString = valueAsString;
		try {
			this.jsonNode = new ObjectMapper().readValue(valueAsString, JsonNode.class);
		} catch (IOException e) {
			throw new RuntimeException("Error trying to read json string", e); 
		}

	}
}
