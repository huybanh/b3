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
	
	//private final int totalCount;
	
	//private int totalProcessedCount;

	private final HashMap<String, HashMap<Long, Entity>> masterMap;
	//private final HashMap<Long, Long> eventPartToEventMap;
	
	public static LinkedList<String> linkingErrors = new LinkedList<String>();
	
	/*private abstract class InitialTask implements Runnable {
		
		//int processedCount;
		
		//int subTotalCount;
	}*/
	
	@SuppressWarnings("unchecked")
	private final ArrayList<Runnable>[] allTasks = new ArrayList[] {
			//entity
			new ArrayList<Runnable>(),
			//event
			new ArrayList<Runnable>(),
			//eventIno
			new ArrayList<Runnable>(),
			//outcome
			new ArrayList<Runnable>(),
			//offer
			new ArrayList<Runnable>()
	};
	
	private ArrayList<Runnable> entityTasks = allTasks[0];
	private ArrayList<Runnable> eventTasks = allTasks[1];
	private ArrayList<Runnable> eventInfoTasks = allTasks[2];
	private ArrayList<Runnable> outcomeTasks = allTasks[3];
	private ArrayList<Runnable> offerTasks = allTasks[4];
	private int taskTypeIndex = 0;
	
	//private ArrayList<Runnable> allTasks = new ArrayList<>();
	
	public InitialDumpDeployer(HashMap<String, HashMap<Long, Entity>> masterMap, int totalCount) {

		this.masterMap = masterMap;
		//this.eventPartToEventMap = eventPartToEventMap;
		//this.totalCount = totalCount;
	}
	
	public void initialPutMaster() {

		//DBTrait db = new FileWorker(new JsonMapper());
		DBTrait db = new DBTrait() {

			private final JsonMapper mapper = new JsonMapper();
			
			@Override
			public void put(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
				DynamoWorker.putFile(mapper, table, hashKey, rangeKey, cells);
			}

			@Override
			public void update(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void delete(B3Table table, String hashKey, String rangeKey) {
				throw new UnsupportedOperationException();
			}
		};
		initialPutAllEntities(db);
		initialPutAllEvents(db);
		initialPutAllEventInfos(db);
		initialPutAllOutcomes(db);
		initialPutAllOffers(db);
		
		int allTaskCount = 0;
		for (ArrayList<?> subList : allTasks) {
			allTaskCount += subList.size();
		}
		final int allTaskCountFinal = allTaskCount;
		
		final ArrayList<Object> threadIds = new ArrayList<Object>();
		//we have 7 files, so 10 threads
		for (int i = 0; i < 10; i++) {
			final Object oneThreadId = new Object();
			threadIds.add(oneThreadId);
			new Thread() {
				public void run() {
					int remainPrint = 0;
					do {
						Runnable oneTask;
						Integer remainTaskCount = null;
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
							oneTask = allTasks[taskTypeIndex].remove(0);
							if (remainPrint == 0) {
								remainTaskCount = 0;
								for (ArrayList<?> subList : allTasks) {
									remainTaskCount += subList.size();
								}
							}
							remainPrint++;
							if (remainPrint == 50) {
								remainPrint = 0;
							}
							//oneTask = allTasks.remove(0);
						}

						if (remainTaskCount != null) {
							logger.info(Thread.currentThread().getName() +  
									": Tasks remain: " + remainTaskCount + " of " + allTaskCountFinal);
						}
						oneTask.run();
						//totalProcessedCount += oneTask.subTotalCount;
						//logger.info("Totally deployed " + totalProcessedCount + " of " + totalCount);
						
					} while (true);
				}
			}.start();
		}
		
		//wait for all initial deploying threads to finish
		while (true) {
			synchronized (allTasks) {
				logger.info("Running initial deploying threads: " + threadIds.size());
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
	
	public void initialPutAllEvents(DBTrait db) {
		
		initialPutAllToMainTable(db, eventTasks, B3Table.Event, Event.class, new B3KeyBuilder<Event>() {

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
	
	public void initialPutAllEventInfos(DBTrait db) {
		
		initialPutAllToMainTable(db, eventInfoTasks, B3Table.EventInfo, EventInfo.class, new B3KeyBuilder<EventInfo>() {

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
	
	public void initialPutAllOutcomes(DBTrait db) {
		
		initialPutAllToMainTable(db, outcomeTasks, B3Table.Outcome, Outcome.class, new B3KeyBuilder<Outcome>() {

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
	
	public void initialPutAllOffers(DBTrait db) {
		
		initialPutAllToMainTable(db, offerTasks, B3Table.BettingOffer, BettingOffer.class, new B3KeyBuilder<BettingOffer>() {

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
			if (i == 50000) {
				subList = null;
				i = 0;
			}
		}
		return parentList.toArray(new ArrayList[parentList.size()]);
	}
	
	public void initialPutAllEntities(final DBTrait db) {

		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			Collection<Entity> entities = entry.getValue().values();
			Collection<Entity>[] subLists = split(entities);
			final int subTotalCount = entities.size();
			final int[] subProcessedCount = new int[] {0};
			//final String subType = entry.getKey();
			final EntitySpec2 spec = EntitySpec2.get(entry.getKey());
			if (spec == null) {
				continue;
			}
			for (Collection<Entity> oneSubList : subLists) {
				final Collection<Entity> oneSubListFinal = oneSubList;
				Runnable oneTask = new Runnable() {
					
					private JsonMapper jsonMapper = new JsonMapper();
					
				    private final Logger logger = Logger.getLogger(this.getClass());
					
					public void run() {
						//final int total = oneSubListFinal.size();
						//int count = 0;
						for (Entity entity : oneSubListFinal) {
							B3KeyEntity entityKey = new B3KeyEntity(entity);
							db.put(B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(), 
									new B3CellString(B3Table.CELL_LOCATOR_THIZ, jsonMapper.serialize(entity)));
						}
						synchronized (subProcessedCount) {
							subProcessedCount[0] += oneSubListFinal.size();
							logger.info(Thread.currentThread().getName() + ": Entity " + spec.shortName + ": deployed " + subProcessedCount[0] + " of " + subTotalCount);
						}
					}
				};
				//oneTask.subTotalCount = entities.size();
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
	private <E extends Entity> void initialPutAllToMainTable(
			final DBTrait db, ArrayList<Runnable> runnerList, 
			final B3Table table, final Class<E> entityClazz, final B3KeyBuilder<E> keyBuilder) {

		final int[] subProcessedCount = new int[] {0};
		final HashMap<Long, Entity> allEntities = masterMap.get(entityClazz.getName());
		Collection<Entity>[] subLists = split(allEntities.values());
		for (Collection<Entity> oneSubList : subLists) {
			final Collection<Entity> oneSubListFinal = oneSubList;
			Runnable oneTask = new Runnable() {
				
				private JsonMapper jsonMapper = new JsonMapper();
				
			    private final Logger logger = Logger.getLogger(this.getClass());
				
				public void run() {
					//int entityCount = oneSubListFinal.size();
					//int count = 0;
					for (Entity entity : oneSubListFinal) {
						//processedCount++;
						/*if (count < start) {
							continue;
						}
						if (end != null && start + count > end) {
							break;
						}*/
						B3Entity<E> b3entity = keyBuilder.newB3Entity();
						b3entity.entity = (E) entity;
						b3entity.buildDownlinks(false, masterMap, null);
						B3Key b3key = keyBuilder.buildKey(b3entity);
						
						//put linked entities to table main, lookup, link
						LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
						putToLookupAndLinkRecursively(db, table, b3key, b3Cells, null, b3entity, masterMap, jsonMapper);
						
						//put main entity to main table
						//B3Update update = new B3Update(table, b3key, b3Cells.toArray(new B3CellString[b3Cells.size()]));
						db.put(table, b3key.getHashKey(), b3key.getRangeKey(), 
								b3Cells.toArray(new B3CellString[b3Cells.size()]));
						
						//put main entity revision to main table
						if (b3entity.getSpec().revisioned) {
							b3key.setRevisionId("0");
							db.put(table, b3key.getHashKey(), b3key.getRangeKey(), 
									b3Cells.toArray(new B3CellString[b3Cells.size()]));
						}
						//entity table
						/*B3KeyEntity entityKey = new B3KeyEntity(entity);
						update = new B3Update(B3Table.Entity, entityKey, 
								new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
						update.execute();*/
						
						//if (processedCount % 100 == 0) {
							//logger.info(table.name + ": deployed " + processedCount + " of " + subTotalCount);
						//}
					}
					synchronized (subProcessedCount) {
						subProcessedCount[0] += oneSubListFinal.size();
						logger.info(Thread.currentThread().getName() + ": " + table.name + ": deployed " + subProcessedCount[0] + " of " + allEntities.size());
					}
				}
			};
			//oneTask.subTotalCount = allEntities.size();
			runnerList.add(oneTask);
		}
	}
	
	/**
	 * @param putToFile
	 * @param mainTable
	 * @param mainKey: Null if no actual puts required
	 * @param mainCells
	 * @param cellName
	 * @param b3entity
	 * @param masterMap
	 * @param jsonMapper
	 */
	public static <E extends Entity>void putToLookupAndLinkRecursively(DBTrait db,
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
		if (mainKey != null) {
			B3KeyLookup lookupKey = new B3KeyLookup(
					b3entity.entity, mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), thisCellName);
			db.put(B3Table.Lookup, lookupKey.getHashKey(), lookupKey.getRangeKey());
		}
		
		EntityLink[] linkedEntities = b3entity.getDownlinkedEntities();
		if (linkedEntities != null) {
			for (EntityLink link : linkedEntities) {
				
				//link: From main entity -> linked entities
				if (link.linkedEntity != null) {
					link.linkedEntity.buildDownlinks(false, masterMap, jsonMapper);
				}
				
				//put linked entity to table link
				if (mainKey != null) {
					B3KeyLink linkKey = new B3KeyLink(link.linkedEntityClazz, link.linkedEntityId, b3entity.entity, link.name); //reverse link direction
					db.put(B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
					
					//commented out, as we can always find a link without information from lookup table
					//also, put link to lookup: Main entity -> link location
					//lookupKey = new B3KeyLookup(b3entity.entity, B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
					//update = new B3Update(B3Table.Lookup, lookupKey);
					//DynamoWorker.put(bundleId, update);
				}
				if (link.linkedEntity != null) {
					String childCellName;
					if (cellName == null) {
						childCellName = link.name;
					} else {
						childCellName = cellName + B3Table.CELL_LOCATOR_SEP + link.name;
					}
					putToLookupAndLinkRecursively(
							db, mainTable, mainKey, mainCells, childCellName, link.linkedEntity, masterMap, jsonMapper);
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
