package com.betbrain.b3.data;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.model.B3EventInfo;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;

public class EntityInitialPutHandler {

	private final HashMap<String, HashMap<Long, Entity>> masterMap;
	//private final HashMap<Long, Long> eventPartToEventMap;
	
	public static LinkedList<String> linkingErrors = new LinkedList<String>();
	
	public EntityInitialPutHandler(HashMap<String, HashMap<Long, Entity>> masterMap/*,
			HashMap<Long, Long> eventPartToEventMap*/) {
		
		this.masterMap = masterMap;
		//this.eventPartToEventMap = eventPartToEventMap;
	}
	
	public void initialPutMaster() {
		
		initialPutAll(B3Table.BettingOffer, 0, null, BettingOffer.class, new B3KeyBuilder<BettingOffer>() {

			public B3BettingOffer newB3Entity() {
				return new B3BettingOffer();
			}

			public B3Key buildKey(B3Entity<BettingOffer> b3entity) {
				B3BettingOffer offer = (B3BettingOffer) b3entity;
				return new B3KeyOffer(
						offer.outcome.event.entity.getSportId(),
						offer.outcome.event.entity.getTypeId(),
						false,
						offer.outcome.event.entity.getId(),
						offer.outcome.entity.getTypeId(),
						offer.outcome.entity.getId(),
						offer.entity.getBettingTypeId(),
						offer.entity.getId());
			}
		});
		
		initialPutAll(B3Table.Event, 0, null, Event.class, new B3KeyBuilder<Event>() {

			public B3Entity<Event> newB3Entity() {
				return new B3Event();
			}

			public B3Key buildKey(B3Entity<Event> b3entity) {
				B3Event event = (B3Event) b3entity;
				return new B3KeyEvent(
						event.entity.getSportId(),
						event.entity.getTypeId(),
						false,
						event.entity.getId());
			}
		});
		
		initialPutAll(B3Table.EventInfo, 0, null, EventInfo.class, new B3KeyBuilder<EventInfo>() {

			public B3Entity<EventInfo> newB3Entity() {
				return new B3EventInfo();
			}

			public B3Key buildKey(B3Entity<EventInfo> b3entity) {
				B3EventInfo eventInfo = (B3EventInfo) b3entity;
				return new B3KeyEventInfo(
						eventInfo.event.entity.getSportId(),
						eventInfo.event.entity.getTypeId(),
						false,
						eventInfo.event.entity.getId(),
						eventInfo.entity.getTypeId(),
						eventInfo.entity.getId());
			}
		});

		//final int start = 0;
		//final int end = 100;
		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			/*if (BettingOffer.class.getName().equals(entry.getKey()) ||
					Event.class.getName().equals(entry.getKey())) {
				continue;
			}*/
			int count = 0;
			int total = entry.getValue().size();
			System.out.println(entry.getKey() + ": " + total);
			for (Entity entity : entry.getValue().values()) {
				count++;
				//if (start + count > end) {
				//	break;
				//}
				if (count % 1000 == 0) {
					System.out.println(entity.getClass().getName() + " " + count + " of " + total);
				}
				String shortName = ModelShortName.get(entity.getClass().getName());
				if (shortName == null) {
					continue;
				}
				B3KeyEntity entityKey = new B3KeyEntity(entity);
				B3Update update = new B3Update(B3Table.Entity, entityKey, 
						new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
				update.execute();
			}
		}

		if (linkingErrors.isEmpty()) {
			System.out.println("Completed all initial puts without any linking errors found");
		} else {
			System.out.println("Completed all initial puts with linking errors found:");
			for (String err : linkingErrors) {
				System.out.println(err);
			}
		}
	}
	
	//private LinkedList<String> loggedMissingSpecs = new LinkedList<String>();

	private interface B3KeyBuilder<E extends Entity> {
		B3Entity<E> newB3Entity();
		B3Key buildKey(B3Entity<E> b3entity);
	}
	
	@SuppressWarnings("unchecked")
	private <E extends Entity> void initialPutAll(B3Table table, int start, Integer end,
			Class<E> entityClazz, B3KeyBuilder<E> keyBuilder) {

		HashMap<Long, Entity> allEntities = masterMap.get(entityClazz.getName());
		int entityCount = allEntities.size();
		//final int start = 0;
		//final int end = 100;
		int count = 0;
		for (Entity entity : allEntities.values()) {
			count++;
			if (count < start) {
				continue;
			}
			if (end != null && start + count > end) {
				break;
			}
			System.out.println(entityClazz.getName() + " " + count + " of " + entityCount);
			B3Entity<E> b3entity = keyBuilder.newB3Entity();
			b3entity.entity = (E) entity;
			b3entity.buildDownlinks(masterMap);
			B3Key b3key = keyBuilder.buildKey(b3entity);
			
			LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
			
			//put linked entities to table main, lookup, link
			initialPutOne(table, b3key, b3Cells, null, b3entity);
			
			//put main entity to main table
			B3Update update = new B3Update(table, b3key, b3Cells.toArray(new B3CellString[b3Cells.size()]));
			update.execute();
			
			//entity table
			/*B3KeyEntity entityKey = new B3KeyEntity(entity);
			update = new B3Update(B3Table.Entity, entityKey, 
					new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
			update.execute();*/
		}
	}
	
	private <E extends Entity>void initialPutOne(
			B3Table mainTable, B3Key mainKey, LinkedList<B3Cell<?>> mainCells, final String cellName, B3Entity<?> b3entity) {
		
		String thisCellName;
		if (cellName == null) {
			thisCellName = B3Table.CELL_LOCATOR_THIZ;
		} else {
			thisCellName = cellName;
		}
		
		//put event to main
		B3CellString jsonCell = new B3CellString(thisCellName, JsonMapper.SerializeF(b3entity.entity));
		mainCells.add(jsonCell);
		
		//put event to lookup
		B3KeyLookup lookupKey = new B3KeyLookup(b3entity.entity, mainTable, mainKey.getHashKey(), mainKey.getRangeKey());
		B3Update update = new B3Update(B3Table.Lookup, lookupKey);
		update.execute();
		
		EntityLink[] linkedEntities = b3entity.getDownlinkedEntities();
		if (linkedEntities != null) {
			for (EntityLink link : linkedEntities) {
				
				//link: From main entity -> linked entities
				if (link.linkedEntity != null) {
					link.linkedEntity.buildDownlinks(masterMap);
				}
				
				//put event to table link
				//B3KeyLink linkKey = new B3KeyLink(link.linkedEntity.entity, b3entity.entity, link.name); //reverse link direction
				B3KeyLink linkKey = new B3KeyLink(link.linkedEntityClazz, link.linkedEntityId, b3entity.entity, link.name); //reverse link direction
				update = new B3Update(B3Table.Link, linkKey);
				update.execute();
				
				//also, put link to lookup: Main entity -> link location
				lookupKey = new B3KeyLookup(b3entity.entity, B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
				update = new B3Update(B3Table.Lookup, lookupKey);
				update.execute();

				if (link.linkedEntity != null) {
					String childCellName;
					if (cellName == null) {
						childCellName = link.name;
					} else {
						childCellName = cellName + B3Table.CELL_LOCATOR_SEP + link.name;
					}
					initialPutOne(mainTable, mainKey, mainCells, childCellName, link.linkedEntity);
				}
			}
		}
	}
	
	//private int missedDownlinkCount;
	
	/*private <E extends Entity> void putMain(B3KeyEvent eventMainKey, E entity) {
		
		@SuppressWarnings("unchecked")
		EntitySpec<Entity> spec = (EntitySpec<Entity>) EntitySpecMapping.getSpec(entity.getClass().getName());
		LinkedList<B3Cell<?>> cells = spec.getCellList(entity);
		B3Update update = new B3Update(spec.targetTable, eventMainKey, cells);
		update.execute();
	}*/
	
	public static void main(String[] args) {
		
		//EntitySpecMapping.initialize();
		Event event = new Event();
		event.setId(1099);
		event.setCurrentPartId(100L);
		event.setStartTime(new Date(1234));
		HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();
		HashMap<Long, Entity> subMap = new HashMap<Long, Entity>();
		masterMap.put(Event.class.getName(), subMap);
		subMap.put(event.getId(), event);
		//String json = JsonMapper.Serialize(event);
		new EntityInitialPutHandler(masterMap/*, null*/).initialPutMaster();

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
