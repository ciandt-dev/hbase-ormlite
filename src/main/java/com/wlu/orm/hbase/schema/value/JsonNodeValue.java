package com.wlu.orm.hbase.schema.value;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
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
	
	public static void main(String[] args) throws JsonGenerationException, JsonMappingException, JsonProcessingException, IOException {
//		ObjectMapper mapper = new ObjectMapper();
//		List<String> l = Arrays.asList("b13150add386ccae","3baf93434a5c7be9");
//		JsonNode valueToTree = mapper.valueToTree(l);
//		String writeValueAsString = mapper.writeValueAsString(valueToTree);
//		System.out.println("Create string : "+writeValueAsString);
//		String writeValueAsString2 = mapper.writeValueAsString(mapper.readTree(writeValueAsString));
//		System.out.println(writeValueAsString2);
//		List<String> readValue = 
//		System.out.println(readValue);
	}
	
}
