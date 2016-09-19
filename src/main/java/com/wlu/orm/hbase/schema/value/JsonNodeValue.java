package com.wlu.orm.hbase.schema.value;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonNodeValue implements Value {

	private JsonNode jsonNode;

	public JsonNodeValue(JsonNode jsonNodeValue) {
		super();
		this.jsonNode = jsonNodeValue;
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
}
