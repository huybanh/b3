package com.betbrain.b3.pushclient;

import java.util.Date;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;


public class JsonMapper {

	//private ObjectMapper mapper = new ObjectMapper();
	
	@SuppressWarnings("rawtypes")
	private JSONDeserializer flexDe;
	private JSONSerializer flexSer;
	
	@SuppressWarnings("rawtypes")
	public JsonMapper() {

		flexDe = new JSONDeserializer();
		ExcludeTransformer excludeTransformer = new ExcludeTransformer();
		flexSer = new JSONSerializer().exclude("beanInfo").transform(excludeTransformer, void.class)
				.include("propertyNames").include("propertyValues");
	}

	public String serialize(Object entity) {
		return flexSer.serialize(entity);
	}
	
	public Entity deserializeEntity(String json) {
		return (Entity) flexDe.deserialize(json);
	}
	
	public Object deserialize(String json) {
		return flexDe.deserialize(json);
	}
	
	/*public String SerializeExcludeClassName(Object entity) {
		String jsonString = "[]";
		jsonString = flexSer.exclude("*.class").serialize(entity);
		return jsonString + ",";
	}
	
	public Entity Deserialize(String json) {
		Entity entity = null;
		try {
			entity = mapper.readValue(json, Entity.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return entity;
	}
	
	public String Serialize(Entity entity) {
		String jsonString = "[]";
		try {
			mapper.enableDefaultTyping(DefaultTyping.NON_FINAL);
			jsonString = mapper.writeValueAsString(entity);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return jsonString;
	}*/
	
	public static void main(String[] args) {
		try {
			System.out.println("Hello Word");
			Event event = new Event();
			event.setCurrentPartId(100L);
			event.setStartTime(new Date(1234));
			String json = new JsonMapper().serialize(event);
			System.out.println(json);			
			Event entity = (Event) new JsonMapper().deserialize(json);
			System.out.println(entity.getCurrentPartId());
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}