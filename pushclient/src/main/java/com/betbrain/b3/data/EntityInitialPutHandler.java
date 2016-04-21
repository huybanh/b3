package com.betbrain.b3.data;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.b3.data.spec.EventSpec;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class EntityInitialPutHandler {

	private HashMap<Long, Entity> sports;

	public void initialPut(HashMap<String, HashMap<Long, Entity>> map) {

		sports = map.get(Sport.class.getName());
		for (Entity e : sports.values()) {
			putLookup(e);
		}
		System.out.println("Completed initial sport puts");
		
		HashMap<Long, Entity> events = map.get(Event.class.getName());
		//int limit = 0;
		for (Entity e : events.values()) {
			initialPut((Event) e);
			//if (limit++ > 20) {
				//break;
			//}
		}
		System.out.println("Completed initial event puts");

		if (errors.isEmpty()) {
			System.out.println("No linking errors found");
		} else {
			for (String err : errors) {
				System.out.println(err);
			}
		}
		System.out.println("Completed initial ALL puts");
	}

	private void initialPut(Event event) {
		
		EventSpec eventSpec = (EventSpec) EntitySpecMapping.getSpec(Event.class.getName());
		B3Key eventMainKey = eventSpec.getB3KeyMain(event);
		LinkedList<B3Cell<?>> eventCells = eventSpec.getCellList(event);
		
		//put event to lookup
		B3Update update = new B3Update(B3Table.Lookup, new B3KeyLookup(event), eventCells);
		update.execute();
		
		//put event to main
		update = new B3Update(eventSpec.targetTable, eventMainKey, eventCells);
		update.execute();
		
		/*SportSpec sportSpec = (SportSpec) EntitySpecMapping.getSpec(Sport.class.getName());
		LinkedList<B3Cell<?>> sportCells = sportSpec.getCellList(sport);
		update = new B3Update(sportSpec.targetTable, eventMainKey, sportCells);
		update.execute();*/
		
		Sport sport = (Sport) sports.get(event.getSportId());
		if (sport == null) {
			errors.add("Missing sport: " + event.getSportId());
		} else {
			putMain(eventMainKey, sport);
		}
	}
	
	private LinkedList<String> errors = new LinkedList<String>();
	
	private <E extends Entity> void putMain(B3Key eventMainKey, E entity) {
		
		@SuppressWarnings("unchecked")
		EntitySpec<Entity> spec = (EntitySpec<Entity>) EntitySpecMapping.getSpec(entity.getClass().getName());
		LinkedList<B3Cell<?>> cells = spec.getCellList(entity);
		B3Update update = new B3Update(spec.targetTable, eventMainKey, cells);
		update.execute();
	}
	
	private <E extends Entity> void putLookup(E entity) {
		
		if (entity == null) {
			return;
		}
		
		@SuppressWarnings("unchecked")
		EntitySpec<Entity> spec = (EntitySpec<Entity>) EntitySpecMapping.getSpec(entity.getClass().getName());
		LinkedList<B3Cell<?>> cells = spec.getCellList(entity);
		B3Update update = new B3Update(B3Table.Lookup, new B3KeyLookup(entity), cells);
		update.execute();
	}
	
	public static void main(String[] args) {
		
		EntitySpecMapping.initialize();
		Event event = new Event();
		event.setId(1099);
		event.setCurrentPartId(100L);
		event.setStartTime(new Date(1234));
		//String json = JsonMapper.Serialize(event);
		new EntityInitialPutHandler().initialPut(event);

		/*
		UPDATE lookup: (0, EV/1099), EVsportId:long 0, EVstatusId:long 0, EVrootPartId:long 0, 
		  EVtypeId:long 0, EVcurrentPartId:long 100, EVid:long 1099
		UPDATE event: (0, 0/0/E1099), EVsportId:long 0, EVstatusId:long 0, EVrootPartId:long 0, 
		  EVtypeId:long 0, EVcurrentPartId:long 100, EVid:long 1099, EV_B3:String 
		  Event[id="1099",typeId="0",isComplete="false",sportId="0",templateId="null",
		  promotionId="null",parentId="null",parentPartId="null",name="null",startTime="Thu Jan 01 08:00:01 ICT 1970",
		  endTime="null",deleteTimeOffset="0",venueId="null",statusId="0",hasLiveStatus="false",
		  rootPartId="0",currentPartId="100",url="null",popularity="null",note="null"]
		 */
	}
}
