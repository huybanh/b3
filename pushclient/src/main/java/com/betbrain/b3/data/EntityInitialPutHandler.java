package com.betbrain.b3.data;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;

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
		
		/*@SuppressWarnings("unchecked")
		EntitySpec<E, ?> spec = (EntitySpec<E, ?>) EntitySpecMapping.getSpec(entity.getClass().getName());
		if (spec == null) {
			if (!loggedMissingSpecs.contains(entity.getClass().getName())) {
				loggedMissingSpecs.add(entity.getClass().getName());
				linkingErrors.add("Missed entity spec for " + entity.getClass().getName());
			}
			return;
		}
		B3Key mainKey = spec.getB3KeyMain(entity);
		if (mainKey == null) {
			mainKey = spec.getB3KeyMainInitially(entity, masterMap, eventPartToEventMap);
		}*/
		
		String thisCellName;
		if (cellName == null) {
			thisCellName = B3Table.CELL_LOCATOR_THIZ;
		} else {
			thisCellName = cellName;
		}
		
		//put event to main
		B3CellString jsonCell = new B3CellString(thisCellName, JsonMapper.SerializeF(b3entity.entity));
		//B3Update update = new B3Update(table, mainKey, jsonCell);
		//update.execute();
		mainCells.add(jsonCell);
		
		//put event to lookup
		//B3CellInt hashCell = new B3CellInt(B3Table.LOOKUP_CELL_TARGET_HASH, mainKey.getHashKey());
		//B3CellString rangeCell = new B3CellString(B3Table.LOOKUP_CELL_TARGET_RANGE, mainKey.getRangeKey());
		B3KeyLookup lookupKey = new B3KeyLookup(b3entity.entity, mainTable, mainKey.getHashKey(), mainKey.getRangeKey());
		B3Update update = new B3Update(B3Table.Lookup, lookupKey);
		update.execute();
		
		EntityLink[] linkedEntities = b3entity.getDownlinkedEntities();
		if (linkedEntities != null) {
			for (EntityLink link : linkedEntities) {
				
				link.linkedEntity.buildDownlinks(masterMap);
				
				//put event to lookup
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
		
		/*LinkedList<EntityLink> downLinks = new LinkedList<EntityLink>();
		spec.getAllDownlinks(entity, downLinks);
		for (EntityLink link : downLinks) {
			HashMap<Long, Entity> subMap = masterMap.get(link.targetClass.getName());
			if (subMap == null) {
				linkingErrors.add("Found zero entities of type: " + link.targetClass.getName());
				continue;
			}
			if (link.targetId == null) {
				continue;
			}
			Entity linkedEntity = subMap.get(link.targetId);
			if (linkedEntity == null) {
				if (missedDownlinkCount < 10) {
					linkingErrors.add("Missed downlink: " + link.targetClass.getName() + "@" + link.targetId);
					missedDownlinkCount++;
					if (missedDownlinkCount == 10) {
						linkingErrors.add("There maybe more missed downlinks but not logged");
					}
				}
				continue;
			}
			
			String childCellName;
			if (cellName == null) {
				childCellName = link.name;
			} else {
				childCellName = cellName + B3Table.CELL_LOCATOR_SEP + link.name;
			}
			initialPut(table, childCellName, linkedEntity);
		}*/
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
