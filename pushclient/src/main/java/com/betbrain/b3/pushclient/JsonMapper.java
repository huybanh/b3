package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.google.gson.JsonDeserializationContext;
import com.betbrain.b3.model.DetailsOddEntity;
import com.betbrain.b3.model.DetailsOddPartEntity;
import com.betbrain.b3.model.ItemProvider;

import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;


public class JsonMapper {

	@SuppressWarnings("rawtypes")
	private JSONDeserializer flexDe;
	private JSONSerializer flexSer;
	
	@SuppressWarnings("rawtypes")
	public JsonMapper() {

		flexDe = new JSONDeserializer();
		//ExcludeTransformer excludeTransformer = new ExcludeTransformer();
		flexSer = new JSONSerializer().exclude("beanInfo")
				.transform(new ExcludeTransformer(), void.class)
				.include("b3PropertyNames")
				.include("b3PropertyValues")
				.include("b3Cells");
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
			
			Date d = new Date();
			System.out.println(d);
			System.out.println(d.getTime());
			BettingOffer o = new BettingOffer();
			o.setLastChangedTime(d);
			System.out.println(o);
			String s = new JsonMapper().serialize(o);
			System.out.println(s);
			o = (BettingOffer) new JsonMapper().deserialize(s);
			System.out.println(o);
			System.out.println(o.getLastChangedTime());
			System.out.println(o.getLastChangedTime().getTime());
			
			DetailsOddPartEntity myentity = new DetailsOddPartEntity("Winner 1");
			JSONSerializer tmp = new JSONSerializer();
			
			HashMap<String, ArrayList<ItemProvider>> data1 = new HashMap<String, ArrayList<ItemProvider>>();
			HashMap<String, ArrayList<ItemProvider>> data2 = new HashMap<String, ArrayList<ItemProvider>>();
			ArrayList<ItemProvider> lst1 = new ArrayList<ItemProvider>();
			lst1.add(new ItemProvider("7.5", "Bet365"));
			lst1.add(new ItemProvider("0-0", "Bet365 Mobile"));
			lst1.add(new ItemProvider("In Progress", "Bet 365 Mobile"));
			data1.put(DateTime.now().toString(), lst1);

			ArrayList<ItemProvider> lst2 = new ArrayList<ItemProvider>();
			lst2.add(new ItemProvider("8.5", "Bet365"));
			lst2.add(new ItemProvider("1-0", "Bet365 Mobile"));
			lst2.add(new ItemProvider("In Progress", "Bet 365 Mobile"));
			data2.put(DateTime.now().toString(), lst2);

			myentity.getRowData().add(data1);
			myentity.getRowData().add(data2);
			
			DetailsOddEntity odds = new DetailsOddEntity();
			odds.getDataReport().add(myentity);
			
			//ExcludeTransformer excludeTransformer = new ExcludeTransformer();
			String str = tmp.exclude("*.class").deepSerialize(odds);
			System.out.println(str);
			//System.out.println(tmp.include("RowData").serialize(myentity));
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}