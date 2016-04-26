package com.betbrain.b3.pushclient;

import java.io.IOException;
import java.util.Date;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectMapper.DefaultTyping;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;


public class JsonMapper {

	private static ObjectMapper mapper = new ObjectMapper();
	private static JSONSerializer flexSer = new JSONSerializer();
	private static JSONDeserializer flexDe = new JSONDeserializer();

	public static String SerializeF(Object entity) {
		String jsonString = "[]";
		jsonString = flexSer.exclude("beanInfo")
				.transform(new ExcludeTransformer(), void.class)
				.serialize(entity);
		return jsonString;
	}
	
	public static String SerializeExcludeClassName(Object entity) {
		String jsonString = "[]";
		jsonString = flexSer.exclude("*.class").serialize(entity);
		return jsonString + ",";
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
	public static String Serialize(Entity entity) {
		String jsonString = "[]";
		try {
			mapper.enableDefaultTyping(DefaultTyping.NON_FINAL);
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
	
	public static Entity DeserializeF(String json) {
		Entity entity = null;
		entity = (Entity)flexDe.deserialize(json);
		return entity;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			System.out.println("Hello Word");
			Event event = new Event();
			event.setCurrentPartId(100L);
			event.setStartTime(new Date(1234));
			String json = JsonMapper.SerializeF(event);
			System.out.println(json);			
			Event entity = (Event)JsonMapper.DeserializeF(json);
			System.out.println(entity.getCurrentPartId());
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}