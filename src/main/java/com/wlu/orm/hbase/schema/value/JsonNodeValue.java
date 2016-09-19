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
//		StringBuilder sb = new StringBuilder();
//		ObjectMapper om = new ObjectMapper();
		String result = "";
		try {
			result = new ObjectMapper().writeValueAsString(jsonNode);
		} catch (IOException e) {
			throw new RuntimeException("Error trying to parse json");
		}
//		jsonNode.getElements().forEachRemaining(a -> {
//			sb.append(jsonNode.asText());
//		});
		return Bytes.toBytes(result);
	}

	@Override
	public String getType() {
		return "JsonNodeValue";
	}

	
	public static void main(String[] args) throws JsonProcessingException, IOException {
		List<String> asList = Arrays.asList("Android","IOS","");
		ObjectMapper mapper = new ObjectMapper();// system:android:AAPPAA1
		JsonNode jsonnode = mapper.valueToTree(asList);
//		final String json = "[\"One\", \"Two\", \"Three\"]";
		 
		String readValue = new ObjectMapper().writeValueAsString(jsonnode);
		
		System.out.println(readValue);
		
//		List<String> items = Arrays.asList(readValue);
		JsonNode jsonnodae = mapper.valueToTree(readValue);
		System.out.println(jsonnodae.asText());
//		if (arrNode.isArray()) {
//		    for (final JsonNode objNode : arrNode) {
//		        System.out.println(objNode);
//		    } 
//		} 
		
	}
}
