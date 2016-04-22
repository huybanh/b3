package com.betbrain.b3.data;

import java.util.Date;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;

public class EntityUpdateHandler<E extends Entity> {

	public void update(E entity) {
		
		/*@SuppressWarnings("unchecked")
		EntitySpec<E> spec = (EntitySpec<E>) EntitySpecMapping.getSpec(entity.getClass().getName());
		LinkedList<B3Cell<?>> allCells = spec.getCellList(entity);
		
		B3KeyLookup lookupKey = new B3KeyLookup(entity);
		B3Update update = new B3Update(B3Table.Lookup, lookupKey, allCells);
		update.execute();
		
		B3Key mainKey = spec.getB3KeyMain(entity);
		if (mainKey.isDetermined()) {
			update = new B3Update(spec.targetTable, mainKey, allCells);
			update.execute();
		}*/
	}
	
	public static void main(String[] args) {
		
		EntitySpecMapping.initialize();
		Event event = new Event();
		event.setId(1099);
		event.setCurrentPartId(100L);
		event.setStartTime(new Date(1234));
		//String json = JsonMapper.Serialize(event);
		new EntityUpdateHandler<Entity>().update(event);

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
