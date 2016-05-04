package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

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

public class InitialDumpDeployer {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	private final int totalCount;
	
	private int totalProcessedCount;

	private final HashMap<String, HashMap<Long, Entity>> masterMap;
	//private final HashMap<Long, Long> eventPartToEventMap;
	
	public static LinkedList<String> linkingErrors = new LinkedList<String>();
	
	private abstract class InitialTask implements Runnable {
		
		int processedCount;
		
		int subTotalCount;
	}
	
	@SuppressWarnings("unchecked")
	private final LinkedList<InitialTask>[] allTasks = new LinkedList[] {
			//entity
			new LinkedList<InitialTask>(),
			//event
			new LinkedList<InitialTask>(),
			//eventIno
			new LinkedList<InitialTask>(),
			//outcome
			new LinkedList<InitialTask>(),
			//offer
			new LinkedList<InitialTask>()
	};
	
	private LinkedList<InitialTask> entityTasks = allTasks[0];
	private LinkedList<InitialTask> eventTasks = allTasks[1];
	private LinkedList<InitialTask> eventInfoTasks = allTasks[2];
	private LinkedList<InitialTask> outcomeTasks = allTasks[3];
	private LinkedList<InitialTask> offerTasks = allTasks[4];
	private int taskTypeIndex = 0;
	
	public InitialDumpDeployer(HashMap<String, HashMap<Long, Entity>> masterMap, int totalCount) {

		this.masterMap = masterMap;
		//this.eventPartToEventMap = eventPartToEventMap;
		this.totalCount = totalCount;
	}
	
	public void initialPutMaster(int threads) {

		initialPutAllEntities();
		initialPutAllEvents();
		initialPutAllEventInfos();
		initialPutAllOutcomes();
		initialPutAllOffers();
		
		final ArrayList<Object> threadIds = new ArrayList<Object>();
		for (int i = 0; i < threads; i++) {
			final Object oneThreadId = new Object();
			threadIds.add(oneThreadId);
			new Thread() {
				public void run() {
					do {
						InitialTask oneTask;
						synchronized (allTasks) {
							int taskTypeCount = 0;
							do {
								taskTypeIndex++;
								if (taskTypeIndex == allTasks.length) {
									taskTypeIndex = 0;
								}
								if (!allTasks[taskTypeIndex].isEmpty()) {
									break;
								}

								taskTypeCount++;
								if (taskTypeCount > allTasks.length + 1) {
									//no more runners
									threadIds.remove(oneThreadId);
									allTasks.notifyAll();
									return;
								}
							} while (true);
							oneTask = allTasks[taskTypeIndex].remove();
						}
						
						oneTask.run();
						totalProcessedCount += oneTask.subTotalCount;
						logger.info("Totally deployed " + totalProcessedCount + " of " + totalCount);
						
					} while (true);
				}
			}.start();
		}
		
		//wait for all initial deploying threads to finish
		while (true) {
			synchronized (allTasks) {
				if (!threadIds.isEmpty()) {
					try {
						allTasks.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
			}
			break;
		}
		
		//all initial-dump deploying threads have finished
		return;
	}
	
	public void initialPutAllEvents() {
		
		initialPutAllToMainTable(eventTasks, B3Table.Event, Event.class, new B3KeyBuilder<Event>() {

			public B3Entity<Event> newB3Entity() {
				return new B3Event();
			}

			public B3Key buildKey(B3Entity<Event> b3entity) {
				B3Event event = (B3Event) b3entity;
				return new B3KeyEvent(
						event.entity.getSportId(),
						event.entity.getTypeId(),
						//false,
						event.entity.getId());
			}
		});
		
	}
	
	public void initialPutAllEventInfos() {
		
		initialPutAllToMainTable(eventInfoTasks, B3Table.EventInfo, EventInfo.class, new B3KeyBuilder<EventInfo>() {

			public B3Entity<EventInfo> newB3Entity() {
				return new B3EventInfo();
			}

			public B3Key buildKey(B3Entity<EventInfo> b3entity) {
				B3EventInfo eventInfo = (B3EventInfo) b3entity;
				return new B3KeyEventInfo(
						eventInfo.event.entity.getSportId(),
						eventInfo.event.entity.getTypeId(),
						//false,
						eventInfo.event.entity.getId(),
						eventInfo.entity.getTypeId(),
						eventInfo.entity.getId());
			}
		});
		
	}
	
	public void initialPutAllOutcomes() {
		
		initialPutAllToMainTable(outcomeTasks, B3Table.Outcome, Outcome.class, new B3KeyBuilder<Outcome>() {

			public B3Outcome newB3Entity() {
				return new B3Outcome();
			}

			public B3Key buildKey(B3Entity<Outcome> b3entity) {
				B3Outcome outcome = (B3Outcome) b3entity;
				return new B3KeyOutcome(
						outcome.event.entity.getSportId(),
						outcome.event.entity.getTypeId(),
						//false,
						outcome.event.entity.getId(),
						outcome.entity.getEventPartId(),
						outcome.entity.getTypeId(),
						outcome.entity.getId());
			}
		});
		
	}
	
	public void initialPutAllOffers() {
		
		initialPutAllToMainTable(offerTasks, B3Table.BettingOffer, BettingOffer.class, new B3KeyBuilder<BettingOffer>() {

			public B3BettingOffer newB3Entity() {
				return new B3BettingOffer();
			}

			public B3Key buildKey(B3Entity<BettingOffer> b3entity) {
				B3BettingOffer offer = (B3BettingOffer) b3entity;
				return new B3KeyOffer(
						offer.outcome.event.entity.getSportId(),
						offer.outcome.event.entity.getTypeId(),
						//false,
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
				InitialTask oneTask = new InitialTask() {
					
					private JsonMapper jsonMapper = new JsonMapper();
					
				    private final Logger logger = Logger.getLogger(this.getClass());
					
					public void run() {
						//final int total = oneSubListFinal.size();
						//int count = 0;
						for (Entity entity : oneSubListFinal) {
							processedCount++;
							String shortName = ModelShortName.get(entity.getClass().getName());
							if (shortName == null) {
								continue;
							}
							//count++;
							if (processedCount % 1000 == 0) {
								logger.info("Entity " + shortName+ ": deployed " + processedCount + " of " + subTotalCount);
							}
							B3KeyEntity entityKey = new B3KeyEntity(entity);
							B3Update update = new B3Update(B3Table.Entity, entityKey, 
									new B3CellString(B3Table.CELL_LOCATOR_THIZ, jsonMapper.serialize(entity)));
							DynamoWorker.put(update);
						}
					}
				};
				oneTask.subTotalCount = entities.size();
				entityTasks.add(oneTask);
			}
		}
	}
	
	//private LinkedList<String> loggedMissingSpecs = new LinkedList<String>();

	private interface B3KeyBuilder<E extends Entity> {
		B3Entity<E> newB3Entity();
		B3Key buildKey(B3Entity<E> b3entity);
	}
	
	@SuppressWarnings("unchecked")
	private <E extends Entity> void initialPutAllToMainTable(LinkedList<InitialTask> runnerList, 
			final B3Table table, /*final int start, final Integer end,*/
			final Class<E> entityClazz, final B3KeyBuilder<E> keyBuilder) {

		HashMap<Long, Entity> allEntities = masterMap.get(entityClazz.getName());
		Collection<Entity>[] subLists = split(allEntities.values());
		for (Collection<Entity> oneSubList : subLists) {
			final Collection<Entity> oneSubListFinal = oneSubList;
			InitialTask oneTask = new InitialTask() {
				
				private JsonMapper jsonMapper = new JsonMapper();
				
			    private final Logger logger = Logger.getLogger(this.getClass());
				
				public void run() {
					//int entityCount = oneSubListFinal.size();
					//int count = 0;
					for (Entity entity : oneSubListFinal) {
						processedCount++;
						/*if (count < start) {
							continue;
						}
						if (end != null && start + count > end) {
							break;
						}*/
						B3Entity<E> b3entity = keyBuilder.newB3Entity();
						b3entity.entity = (E) entity;
						b3entity.buildDownlinks(masterMap, null);
						B3Key b3key = keyBuilder.buildKey(b3entity);
						
						//put linked entities to table main, lookup, link
						LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
						putToMainAndLookupAndLinkRecursively(table, b3key, b3Cells, null, b3entity, masterMap, jsonMapper);
						
						//put main entity to main table
						B3Update update = new B3Update(table, b3key, b3Cells.toArray(new B3CellString[b3Cells.size()]));
						DynamoWorker.put(update);
						
						//entity table
						/*B3KeyEntity entityKey = new B3KeyEntity(entity);
						update = new B3Update(B3Table.Entity, entityKey, 
								new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
						update.execute();*/
						
						if (processedCount % 100 == 0) {
							logger.info(table.name + ": deployed " + processedCount + " of " + subTotalCount);
						}
					}
					
				}
			};
			oneTask.subTotalCount = allEntities.size();
			runnerList.add(oneTask);
		}
	}
	
	public static <E extends Entity>void putToMainAndLookupAndLinkRecursively(
			B3Table mainTable, B3Key mainKey, LinkedList<B3Cell<?>> mainCells, 
			final String cellName, B3Entity<?> b3entity, 
			HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper jsonMapper) {
		
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
		B3KeyLookup lookupKey = new B3KeyLookup(
				b3entity.entity, mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), thisCellName);
		B3Update update = new B3Update(B3Table.Lookup, lookupKey);
		DynamoWorker.put(update);
		
		EntityLink[] linkedEntities = b3entity.getDownlinkedEntities();
		if (linkedEntities != null) {
			for (EntityLink link : linkedEntities) {
				
				//link: From main entity -> linked entities
				if (link.linkedEntity != null) {
					link.linkedEntity.buildDownlinks(masterMap, jsonMapper);
				}
				
				//put linked entity to table link
				//B3KeyLink linkKey = new B3KeyLink(link.linkedEntity.entity, b3entity.entity, link.name); //reverse link direction
				B3KeyLink linkKey = new B3KeyLink(link.linkedEntityClazz, link.linkedEntityId, b3entity.entity, link.name); //reverse link direction
				update = new B3Update(B3Table.Link, linkKey);
				DynamoWorker.put(update);
				
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
					putToMainAndLookupAndLinkRecursively(mainTable, mainKey, mainCells, childCellName, link.linkedEntity, 
							masterMap, jsonMapper);
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
		//new InitialPutHandler(masterMap).initialPutAllEntities();

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
