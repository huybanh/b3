package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.betbrain.b3.model.B3BettingOffer;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.model.B3EventInfo;
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.Outcome;

public class InitialPutHandler {
	
	//private static final int CONCURRENT_FACTOR = 20;
	
	private final B3Bundle bundle;

	private final HashMap<String, HashMap<Long, Entity>> masterMap;
	//private final HashMap<Long, Long> eventPartToEventMap;
	
	public static LinkedList<String> linkingErrors = new LinkedList<String>();
	
	@SuppressWarnings("unchecked")
	private final LinkedList<Runnable>[] runners = new LinkedList[] {
			//entity
			new LinkedList<Runnable>(),
			//event
			new LinkedList<Runnable>(),
			//eventIno
			new LinkedList<Runnable>(),
			//outcome
			new LinkedList<Runnable>(),
			//offer
			new LinkedList<Runnable>()
	};
	
	private LinkedList<Runnable> entityRunners = runners[0];
	private LinkedList<Runnable> eventRunners = runners[1];
	private LinkedList<Runnable> eventInfoRunners = runners[2];
	private LinkedList<Runnable> outcomeRunners = runners[3];
	private LinkedList<Runnable> offerRunners = runners[4];
	private int runnerTypeIndex = 0;
	
	public InitialPutHandler(HashMap<String, HashMap<Long, Entity>> masterMap/*,
			HashMap<Long, Long> eventPartToEventMap*/) {
		
		this.masterMap = masterMap;
		//this.eventPartToEventMap = eventPartToEventMap;
		
		bundle = DynamoWorker.getBundleUnused(DynamoWorker.BUNDLE_STATUS_INITIALDUMP);
	}
	
	public void initialPutMaster() {

		initialPutAllEntities();
		initialPutAllEvents();
		initialPutAllEventInfos();
		initialPutAllOutcomes();
		initialPutAllOffers();
		
		//System.out.println("Total runner count: " + runners.size());
		for (int i = 0; i < 50; i++) {
			new Thread() {
				public void run() {
					do {
						Runnable oneRunner;
						synchronized (runners) {
							int runnerTypeCount = 0;
							do {
								runnerTypeIndex++;
								if (runnerTypeIndex == runners.length) {
									runnerTypeIndex = 0;
								}
								if (!runners[runnerTypeIndex].isEmpty()) {
									break;
								}

								runnerTypeCount++;
								if (runnerTypeCount > runners.length + 1) {
									//no more runners
									return;
								}
							} while (true);
							oneRunner = runners[runnerTypeIndex].remove();
						}
						oneRunner.run();
					} while (true);
				}
			}.start();
		}

		/*if (linkingErrors.isEmpty()) {
			System.out.println("Completed all initial puts without any linking errors found");
		} else {
			System.out.println("Completed all initial puts with linking errors found:");
			for (String err : linkingErrors) {
				System.out.println(err);
			}
		}*/
	}
	
	public void initialPutAllEvents() {
		
		initialPutAll(eventRunners, B3Table.Event, 0, null, Event.class, new B3KeyBuilder<Event>() {

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
		
	}
	
	public void initialPutAllEventInfos() {
		
		initialPutAll(eventInfoRunners, B3Table.EventInfo, 0, null, EventInfo.class, new B3KeyBuilder<EventInfo>() {

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
		
	}
	
	public void initialPutAllOutcomes() {
		
		initialPutAll(outcomeRunners, B3Table.Outcome, 0, null, Outcome.class, new B3KeyBuilder<Outcome>() {

			public B3Outcome newB3Entity() {
				return new B3Outcome();
			}

			public B3Key buildKey(B3Entity<Outcome> b3entity) {
				B3Outcome outcome = (B3Outcome) b3entity;
				return new B3KeyOutcome(
						outcome.event.entity.getSportId(), //TODO what if outcome is eventPart based
						outcome.event.entity.getTypeId(),
						false,
						outcome.event.entity.getId(),
						outcome.entity.getTypeId(),
						outcome.entity.getId());
			}
		});
		
	}
	
	public void initialPutAllOffers() {
		
		initialPutAll(offerRunners, B3Table.BettingOffer, 0, null, BettingOffer.class, new B3KeyBuilder<BettingOffer>() {

			public B3BettingOffer newB3Entity() {
				return new B3BettingOffer();
			}

			public B3Key buildKey(B3Entity<BettingOffer> b3entity) {
				B3BettingOffer offer = (B3BettingOffer) b3entity;
				return new B3KeyOffer(
						offer.outcome.event.entity.getSportId(), //TODO what if outcome is eventPart based
						offer.outcome.event.entity.getTypeId(),
						false,
						offer.outcome.event.entity.getId(),
						offer.outcome.entity.getTypeId(),
						offer.outcome.entity.getId(),
						offer.entity.getBettingTypeId(),
						offer.entity.getId());
			}
		});
		
	}
	
	@SuppressWarnings("unchecked")
	private static <E> Collection<E>[] split(Collection<E> coll) {
		/*if (coll.size() < 1000) {
			return new Collection[] {coll};
		}*/
		
		int i = 0;
		LinkedList<ArrayList<E>> parentList = new LinkedList<ArrayList<E>>();
		ArrayList<E> subList = null;
		for (E e : coll) {
			if (subList == null) {
				subList = new ArrayList<E>();
				parentList.add(subList);
			}
			subList.add(e);
			i++;
			if (i == 1000) {
				subList = null;
				i = 0;
			}
		}
		return parentList.toArray(new ArrayList[parentList.size()]);
	}
	
	public void initialPutAllEntities() {

		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			Collection<Entity> entities = entry.getValue().values();
			Collection<Entity>[] subLists = split(entities);
			for (Collection<Entity> oneSubList : subLists) {
				final Collection<Entity> oneSubListFinal = oneSubList;
				entityRunners.add(new Runnable() {
					
					private JsonMapper jsonMapper = new JsonMapper();
					
					public void run() {
						final int total = oneSubListFinal.size();
						int count = 0;
						for (Entity entity : oneSubListFinal) {
							String shortName = ModelShortName.get(entity.getClass().getName());
							if (shortName == null) {
								continue;
							}
							count++;
							if (count % 1000 == 0) {
								System.out.println("Entity " + 
										ModelShortName.get(entity.getClass().getName()) + " " + count + " of " + total);
							}
							B3KeyEntity entityKey = new B3KeyEntity(entity);
							B3Update update = new B3Update(B3Table.Entity, entityKey, 
									new B3CellString(B3Table.CELL_LOCATOR_THIZ, jsonMapper.serialize(entity)));
							DynamoWorker.put(bundle, update);
						}
					}
				});
			}
		}
	}
	
	//private LinkedList<String> loggedMissingSpecs = new LinkedList<String>();

	private interface B3KeyBuilder<E extends Entity> {
		B3Entity<E> newB3Entity();
		B3Key buildKey(B3Entity<E> b3entity);
	}
	
	@SuppressWarnings("unchecked")
	private <E extends Entity> void initialPutAll(LinkedList<Runnable> runnerList, final B3Table table, final int start, final Integer end,
			final Class<E> entityClazz, final B3KeyBuilder<E> keyBuilder) {

		HashMap<Long, Entity> allEntities = masterMap.get(entityClazz.getName());
		Collection<Entity>[] subLists = split(allEntities.values());
		for (Collection<Entity> oneSubList : subLists) {
			final Collection<Entity> oneSubListFinal = oneSubList;
			runnerList.add(new Runnable() {
				
				private JsonMapper jsonMapper = new JsonMapper();
				
				public void run() {
					int entityCount = oneSubListFinal.size();
					int count = 0;
					for (Entity entity : oneSubListFinal) {
						count++;
						if (count < start) {
							continue;
						}
						if (end != null && start + count > end) {
							break;
						}
						B3Entity<E> b3entity = keyBuilder.newB3Entity();
						b3entity.entity = (E) entity;
						b3entity.buildDownlinks(masterMap);
						B3Key b3key = keyBuilder.buildKey(b3entity);
						
						LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
						
						//put linked entities to table main, lookup, link
						initialPutOne(table, b3key, b3Cells, null, b3entity, jsonMapper);
						
						//put main entity to main table
						B3Update update = new B3Update(table, b3key, b3Cells.toArray(new B3CellString[b3Cells.size()]));
						DynamoWorker.put(bundle, update);
						
						//entity table
						/*B3KeyEntity entityKey = new B3KeyEntity(entity);
						update = new B3Update(B3Table.Entity, entityKey, 
								new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
						update.execute();*/
						
						if (count % 100 == 0) {
							System.out.println(entityClazz.getName() + " " + count + " of " + entityCount);
						}
					}
					
				}
			});
		}
	}
	
	private <E extends Entity>void initialPutOne(
			B3Table mainTable, B3Key mainKey, LinkedList<B3Cell<?>> mainCells, 
			final String cellName, B3Entity<?> b3entity, JsonMapper jsonMapper) {
		
		String thisCellName;
		if (cellName == null) {
			thisCellName = B3Table.CELL_LOCATOR_THIZ;
		} else {
			thisCellName = cellName;
		}
		
		//put event to main
		B3CellString jsonCell = new B3CellString(thisCellName, jsonMapper.serialize(b3entity.entity));
		mainCells.add(jsonCell);
		
		//put event to lookup
		B3KeyLookup lookupKey = new B3KeyLookup(b3entity.entity, mainTable, mainKey.getHashKey(), mainKey.getRangeKey());
		B3Update update = new B3Update(B3Table.Lookup, lookupKey);
		DynamoWorker.put(bundle, update);
		
		EntityLink[] linkedEntities = b3entity.getDownlinkedEntities();
		if (linkedEntities != null) {
			for (EntityLink link : linkedEntities) {
				
				//link: From main entity -> linked entities
				if (link.linkedEntity != null) {
					link.linkedEntity.buildDownlinks(masterMap);
				}
				
				//put linked entity to table link
				//B3KeyLink linkKey = new B3KeyLink(link.linkedEntity.entity, b3entity.entity, link.name); //reverse link direction
				B3KeyLink linkKey = new B3KeyLink(link.linkedEntityClazz, link.linkedEntityId, b3entity.entity, link.name); //reverse link direction
				update = new B3Update(B3Table.Link, linkKey);
				DynamoWorker.put(bundle, update);
				
				//commented out, as we can always find a link without information from lookup table
				//also, put link to lookup: Main entity -> link location
				//lookupKey = new B3KeyLookup(b3entity.entity, B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
				//update = new B3Update(B3Table.Lookup, lookupKey);
				//DynamoWorker.put(bundleId, update);

				if (link.linkedEntity != null) {
					String childCellName;
					if (cellName == null) {
						childCellName = link.name;
					} else {
						childCellName = cellName + B3Table.CELL_LOCATOR_SEP + link.name;
					}
					initialPutOne(mainTable, mainKey, mainCells, childCellName, link.linkedEntity, jsonMapper);
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
		//new EntityInitialPutHandler(masterMap).initialPutMaster();
		new InitialPutHandler(masterMap).initialPutAllEntities();

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
