package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;

public class InitialDumpDeployer {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	//private final int totalCount;
	
	//private int totalProcessedCount;

	private final HashMap<String, HashMap<Long, Entity>> masterMap;
	
	public static LinkedList<String> linkingErrors = new LinkedList<String>();
	
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
			new ArrayList<Runnable>(),
			//event-participant relations
			new ArrayList<Runnable>()
	};
	
	private ArrayList<Runnable> entityTasks = allTasks[0];
	private ArrayList<Runnable> eventTasks = allTasks[1];
	private ArrayList<Runnable> eventInfoTasks = allTasks[2];
	private ArrayList<Runnable> outcomeTasks = allTasks[3];
	private ArrayList<Runnable> offerTasks = allTasks[4];
	private ArrayList<Runnable> epRelationTasks = allTasks[5];
	private int taskTypeIndex = 0;
	
	//private ArrayList<Runnable> allTasks = new ArrayList<>();
	
	public InitialDumpDeployer(HashMap<String, HashMap<Long, Entity>> masterMap) {

		this.masterMap = masterMap;
		//this.eventPartToEventMap = eventPartToEventMap;
		//this.totalCount = totalCount;
	}
	
	/**
	 * Put all entities from initial dump to b3, in 2 stages:
	 * 
	 * - Put to local files
	 * - From local files, put to dynamodb to equally distribute work-load among db's partitions
	 * 
	 */
	public void initialPutMaster() {

		DynamoWorker.openLocalWriters();
		//DBTrait db = new FileWorker(new JsonMapper());
		DBTrait db = new DBTrait() {

			private final JsonMapper mapper = new JsonMapper();
			
			@Override
			public void put(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
				/*if (rangeKey == null) {
					throw new RuntimeException();
				}*/
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
		initialPutAllToMainTable(db, eventTasks, EntitySpec2.Event);
		initialPutAllToMainTable(db, offerTasks, EntitySpec2.BettingOffer);
		initialPutAllToMainTable(db, outcomeTasks, EntitySpec2.Outcome);
		initialPutAllToMainTable(db, eventInfoTasks, EntitySpec2.EventInfo);
		initialPutAllToMainTable(db, epRelationTasks, EntitySpec2.EventParticipantRelation);
		
		int allTaskCount = 0;
		for (ArrayList<?> subList : allTasks) {
			allTaskCount += subList.size();
		}
		final int allTaskCountFinal = allTaskCount;
		
		final ArrayList<Object> threadIds = new ArrayList<Object>();
		//we have 7 files, so 20 threads
		for (int i = 0; i < 10; i++) {
			final Object oneThreadId = new Object();
			threadIds.add(oneThreadId);
			new Thread("LocalPut-thread-" + i) {
				public void run() {
					//int remainPrint = 0;
					do {
						Runnable oneTask;
						int remainTaskCount = 0;
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
							//if (remainPrint == 0) {
								//remainTaskCount = 0;
								for (ArrayList<?> subList : allTasks) {
									remainTaskCount += subList.size();
								}
							//}
							/*remainPrint++;
							if (remainPrint == 50) {
								remainPrint = 0;
							}*/
							//oneTask = allTasks.remove(0);
						}

						//if (remainTaskCount != null) {
							logger.info(Thread.currentThread().getName() +  
									": Tasks remain: " + remainTaskCount + " of " + allTaskCountFinal);
						//}
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
	
	/*@SuppressWarnings("unchecked")
	private static <E> Collection<E>[] split(Collection<E> coll) {
		if (coll.size() < 1000) {
			return new Collection[] {coll};
		}
		
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
			if (i == 100000) {
				subList = null;
				i = 0;
			}
		}
		return parentList.toArray(new ArrayList[parentList.size()]);
	}*/
	
	/**
	 * Put all entities to table entity
	 * 
	 * @param db
	 */
	private void initialPutAllEntities(final DBTrait db) {

		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			final Collection<Entity> entities = entry.getValue().values();
			//Collection<Entity>[] subLists = split(entities);
			final int subTotalCount = entities.size();
			final int[] subProcessedCount = new int[] {0};
			//final String subType = entry.getKey();
			final EntitySpec2 spec = EntitySpec2.get(entry.getKey());
			if (spec == null) {
				continue;
			}
			//for (Collection<Entity> oneSubList : subLists) {
				//final Collection<Entity> oneSubListFinal = oneSubList;
				Runnable oneTask = new Runnable() {
					
					private JsonMapper jsonMapper = new JsonMapper();
					
				    private final Logger logger = Logger.getLogger(this.getClass());
					
					public void run() {
						//final int total = oneSubListFinal.size();
						//int count = 0;
						//for (Entity entity : oneSubListFinal) {
						for (Entity entity : entities) {
							B3KeyEntity entityKey = new B3KeyEntity(entity);
							db.put(B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(), 
									new B3CellString(B3Table.CELL_LOCATOR_THIZ, jsonMapper.serialize(entity)));
						}
						synchronized (subProcessedCount) {
							subProcessedCount[0] += entities.size();
							logger.info(Thread.currentThread().getName() + ": Entity " + spec.shortName + ": deployed " + subProcessedCount[0] + " of " + subTotalCount);
						}
					}
				};
				//oneTask.subTotalCount = entities.size();
				entityTasks.add(oneTask);
			//}
		}
	}
	
	//private LinkedList<String> loggedMissingSpecs = new LinkedList<String>();

	/*private interface B3KeyBuilder<E extends Entity> {
		B3Entity<E> newB3Entity();
		B3Key buildKey(B3Entity<E> b3entity);
	}*/
	
	/**
	 * put main entities to their own tables. The entities are:
	 *   - Event
	 *   - EventInfo
	 *   - Outcome
	 *   - BettingOffer
	 *   - EventParticipantRelation
	 *   
	 * Also, generate and put linking data to the link table. Link data are defined and retrived
	 * from concrete B3Entity classes
	 * 
	 * @param db
	 * @param runnerList
	 * @param spec
	 */
	@SuppressWarnings("unchecked")
	private <E extends Entity> void initialPutAllToMainTable(
			final DBTrait db, ArrayList<Runnable> runnerList,  final EntitySpec2 spec
			/*final B3Table table, final Class<E> entityClazz, final B3KeyBuilder<E> keyBuilder*/) {

		final int[] subProcessedCount = new int[] {0};
		final HashMap<Long, Entity> allEntities = masterMap.get(spec.entityClass.getName());
		if (allEntities == null) {
			return;
		}
		//Collection<Entity>[] subLists = split(allEntities.values());
		final Collection<Entity> entityList = allEntities.values();
		final B3Table table = spec.mainTable;
		//for (Collection<Entity> oneSubList : subLists) {
			//final Collection<Entity> oneSubListFinal = oneSubList;
			Runnable oneTask = new Runnable() {
				
				private JsonMapper jsonMapper = new JsonMapper();
				
			    private final Logger logger = Logger.getLogger(this.getClass());
				
				public void run() {
					//int entityCount = oneSubListFinal.size();
					//int count = 0;
					for (Entity entity : entityList) {
						//processedCount++;
						/*if (count < start) {
							continue;
						}
						if (end != null && start + count > end) {
							break;
						}*/
						//B3Entity<E> b3entity = keyBuilder.newB3Entity();
						B3Entity<E> b3entity = (B3Entity<E>) spec.newB3Entity();
						b3entity.entity = (E) entity;
						b3entity.buildDownlinks(false, masterMap, null);
						B3Key b3key = b3entity.createMainKey();
						
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
						subProcessedCount[0] += entityList.size();
						logger.info(Thread.currentThread().getName() + ": " + 
								table.name + ": deployed " + subProcessedCount[0] + " of " + allEntities.size());
					}
				}
			};
			//oneTask.subTotalCount = allEntities.size();
			runnerList.add(oneTask);
		//}
	}
	
	private static boolean useLookupAndLink = false;
	
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
		if (useLookupAndLink && mainKey != null && cellName != null) {
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
				if (useLookupAndLink && mainKey != null) {
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
		
		//cross links: must be after downlinks, for the link building processed first
		LinkedList<EntityLink> crossLinks = b3entity.getCrossLinks();
		if (crossLinks != null) {
			for (EntityLink link : crossLinks) {
				B3KeyLink linkKey = new B3KeyLink(link.linkedEntityClazz, link.linkedEntityId, link.sourceParts);
				db.put(B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
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
