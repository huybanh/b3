package com.betbrain.b3.pushclient;

import java.io.IOException;
import java.util.Date;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;

public class JsonMapper {

	private static ObjectMapper mapper = new ObjectMapper();
	
	public static String Serialize(Entity entity) {
		String jsonString = "[]";
		try {
			jsonString = mapper.writeValueAsString(entity);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jsonString;
	}
	
	public static Entity Deserialize(String json) {
		Entity entity = null;
		try {
			entity = mapper.readValue(json, Entity.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return entity;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Hello Word");
		Event event = new Event();
		event.setCurrentPartId(100L);
		event.setStartTime(new Date(1234));
		String json = JsonMapper.Serialize(event);
		System.out.println(json);
	}

}
