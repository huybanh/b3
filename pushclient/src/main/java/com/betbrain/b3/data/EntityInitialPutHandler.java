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

	public void initialPut() {
		
		//BettingOffer table (and lookup too)
		HashMap<Long, Entity> allOffers = masterMap.get(BettingOffer.class.getName());
		int offerCount = allOffers.size();
		final int start = 0;
		final int end = 100;
		int count = 0;
		for (Entity entity : allOffers.values()) {
			count++;
			if (start + count > end) {
				break;
			}
			System.out.println("Offer " + (start + count) + " of " + offerCount);
			B3BettingOffer offer = new B3BettingOffer();
			offer.entity = (BettingOffer) entity;
			offer.buildDownlinks(masterMap);
			
			B3KeyOffer offerKey = new B3KeyOffer(
					offer.outcome.event.entity.getSportId(),
					offer.outcome.event.entity.getTypeId(),
					false,
					offer.outcome.event.entity.getId(),
					offer.outcome.entity.getTypeId(),
					offer.outcome.entity.getId(),
					offer.entity.getBettingTypeId(),
					offer.entity.getId());
			
			LinkedList<B3Cell<?>> offerCells = new LinkedList<B3Cell<?>>();
			initialPut(B3Table.BettingOffer, offerKey, offerCells, null, offer);
			B3Update update = new B3Update(B3Table.BettingOffer, offerKey, offerCells.toArray(new B3CellString[offerCells.size()]));
			update.execute();
			
			//entity table
			B3KeyEntity entityKey = new B3KeyEntity(entity);
			update = new B3Update(B3Table.Entity, entityKey, 
					new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
			update.execute();
		}
		
		//Event table (and lookup too)
		count = 0;
		HashMap<Long, Entity> allEvents = masterMap.get(Event.class.getName());
		for (Entity entity : allEvents.values()) {
			count++;
			if (start + count > end) {
				break;
			}
			System.out.println("Event " + (start + count) + " of " + offerCount);
			B3Event event = new B3Event();
			event.entity = (Event) entity;
			event.buildDownlinks(masterMap);
			
			B3KeyEvent eventKey = new B3KeyEvent(
					event.entity.getSportId(),
					event.entity.getTypeId(),
					false,
					event.entity.getId());

			LinkedList<B3Cell<?>> eventCells = new LinkedList<B3Cell<?>>();
			initialPut(B3Table.Event, eventKey, eventCells, null, event);
			B3Update update = new B3Update(B3Table.Event, eventKey, eventCells.toArray(new B3CellString[eventCells.size()]));
			update.execute();
			
			//entity table
			B3KeyEntity entityKey = new B3KeyEntity(entity);
			update = new B3Update(B3Table.Entity, entityKey, 
					new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
			update.execute();
		}
		
		//Event table (and lookup too)
		count = 0;
		HashMap<Long, Entity> allEventInfos = masterMap.get(EventInfo.class.getName());
		for (Entity entity : allEventInfos.values()) {
			count++;
			if (start + count > end) {
				break;
			}
			System.out.println("EventInfo " + (start + count) + " of " + offerCount);
			B3EventInfo eventInfo = new B3EventInfo();
			eventInfo.entity = (EventInfo) entity;
			eventInfo.buildDownlinks(masterMap);
			
			B3KeyEventInfo eventInfoKey = new B3KeyEventInfo(
					eventInfo.event.entity.getSportId(),
					eventInfo.event.entity.getTypeId(),
					false,
					eventInfo.event.entity.getId(),
					eventInfo.entity.getTypeId(),
					eventInfo.entity.getId());

			LinkedList<B3Cell<?>> eventInfoCells = new LinkedList<B3Cell<?>>();
			initialPut(B3Table.EventInfo, eventInfoKey, eventInfoCells, null, eventInfo);
			B3Update update = new B3Update(B3Table.EventInfo, eventInfoKey, eventInfoCells.toArray(new B3CellString[eventInfoCells.size()]));
			update.execute();
			
			//entity table
			B3KeyEntity entityKey = new B3KeyEntity(entity);
			update = new B3Update(B3Table.Entity, entityKey, 
					new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
			update.execute();
		}
		
		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			if (BettingOffer.class.getName().equals(entry.getKey()) ||
					Event.class.getName().equals(entry.getKey())) {
				continue;
			}
			count = 0;
			for (Entity entity : entry.getValue().values()) {
				count++;
				if (start + count > end) {
					break;
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
	
	private <E extends Entity>void initialPut(
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
				
				link.linkedEntity.buildDownlinks(masterMap);
				
				//put event to table link
				B3KeyLink linkKey = new B3KeyLink(link.linkedEntity.entity, b3entity.entity); //reverse link direction
				update = new B3Update(B3Table.Link, linkKey);
				update.execute();

				String childCellName;
				if (cellName == null) {
					childCellName = link.name;
				} else {
					childCellName = cellName + B3Table.CELL_LOCATOR_SEP + link.name;
				}
				initialPut(mainTable, mainKey, mainCells, childCellName, link.linkedEntity);
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
		new EntityInitialPutHandler(masterMap/*, null*/).initialPut();

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
