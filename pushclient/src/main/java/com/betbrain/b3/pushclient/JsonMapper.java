package com.betbrain.b3.pushclient;

import java.util.Date;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;


public class JsonMapper {

	@SuppressWarnings("rawtypes")
	private JSONDeserializer flexDe;
	private JSONSerializer flexSer;
	
	@SuppressWarnings("rawtypes")
	public JsonMapper() {

		flexDe = new JSONDeserializer();
		ExcludeTransformer excludeTransformer = new ExcludeTransformer();
		flexSer = new JSONSerializer().exclude("beanInfo").transform(excludeTransformer, void.class);
	}

	public String serialize(Object entity) {
		return flexSer.serialize(entity);
	}
	
	public Entity deserialize(String json) {
		return (Entity) flexDe.deserialize(json);
	}
		
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