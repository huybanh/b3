package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.betbrain.b3.data.B3ItemIterator;
import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.ChangeDistributor;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ChangeBase;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public class ChangeBatchDeployer {
	
    //private final Logger logger = Logger.getLogger(this.getClass());
	
	//private JsonMapper mapper = new JsonMapper();
	
	//private ExecutorService executor;

	//private final ArrayList<?>[] allEntityLists = new ArrayList<?>[EntitySpec2.values().length];
	
	public static void main(String[] args) {
		
		int initialLoadingThreads = Integer.parseInt(args[0]);
		int deployThreads = Integer.parseInt(args[1]);

		//final int threadCount = Integer.parseInt(args[0]);
		if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_PUSH_WAIT)) {
			if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_PUSHING)) {
				Logger.getLogger(ChangeBatchDeployer.class).error("No bundle available for pushing");
				return;
			}
		}
		//DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYING); //for testing only

		HashMap<String, HashMap<Long, Entity>> cachedEntities = loadEntityCache(initialLoadingThreads);
		//dump masterMap
		for (Entry<String, HashMap<Long, Entity>> entry : cachedEntities.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue().size());
		}
		//deployer.masterMap.get(key)

		ChangeDistributor changeDist = new ChangeDistributor(deployThreads, cachedEntities);
		deployChangeBatches(changeDist);
	}
	
	private ChangeBatchDeployer() {
		//executor = Executors.newFixedThreadPool(threadCount);
	}

	private static void deployChangeBatches(ChangeDistributor changeDist) {
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSHING);
		JsonMapper mapper = new JsonMapper();
		//int deployCount = 0;
		while (true) {
			System.out.println("Querying " + B3Table.DIST_FACTOR + " partitions for first change batch id");
			final ArrayList<String> allRangeIds = new ArrayList<String>();
			for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
				B3ItemIterator iter = DynamoWorker.query(B3Table.SEPC, DynamoWorker.SEPC_CHANGEBATCH + dist, 1);
				while (iter.hasNext()) {
					Item item = iter.next();
					allRangeIds.add(item.getString(DynamoWorker.RANGE));
				}
			}
			Collections.sort(allRangeIds);
			
			//String rangeStart = allRangeIds.get(0);
			long batchId = Long.parseLong(allRangeIds.get(0).split(B3Table.KEY_SEP)[0]);
			System.out.println("Processing batch " + batchId);
			int retryCount = 0;
			while (true) {
				String hashKey = BatchWorker.generateChangeBatchHashKey(batchId);
				ItemCollection<QueryOutcome> coll = DynamoWorker.queryRangeBeginsWith(
						B3Table.SEPC, hashKey, B3Key.zeroPadding(BatchWorker.BATCHID_DIGIT_COUNT, batchId));
				IteratorSupport<Item, QueryOutcome> it = null;
				if (coll != null) {
					it = coll.iterator();
				}

				if (it == null) {
					if (retryCount > 10) {
						break;
					}
					/*try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}*/
					retryCount++;
					continue;
				}
				
				while (it.hasNext()) {
					Item item = it.next();
					String changesJson = item.getString(DynamoWorker.SEPC_CELLNAME_JSON);
					ChangeBase oneChange = (ChangeBase) mapper.deserialize(changesJson);
					oneChange.changeTime = item.getString(DynamoWorker.SEPC_CELLNAME_CREATETIME);
					oneChange.hashKey = hashKey;
					oneChange.rangeKey = item.getString(DynamoWorker.RANGE);
					changeDist.distribute((ChangeBase) oneChange, mapper);
				}
				batchId++;
			}
		}
	}
	
	private static HashMap<String, HashMap<Long, Entity>> loadEntityCache(int loadingThreadCount) {
		
		/*for (int i = 0; i < allEntityLists.length; i++) {
			allEntityLists[i] = new ArrayList<>();
		}*/
		HashMap<String, HashMap<Long, Entity>> cachedEntities = new HashMap<>();
		final ArrayList<EntityLoadingTask> allLoadingTasks = new ArrayList<>();
		for (EntitySpec2 spec : EntitySpec2.values()) {
			final EntitySpec2 specFinal = spec;
			//final ArrayList<Entity> entityList = new ArrayList<>();
			final HashMap<Long, Entity> entityMap = new HashMap<>();
			cachedEntities.put(spec.entityClass.getName(), entityMap);
			System.out.println("Creating loading tasks for " + spec.entityClass.getName());
			for (int partition = 0; partition < B3Table.DIST_FACTOR; partition++) {
				final int partitionFinal = partition;
				allLoadingTasks.add(new EntityLoadingTask() {
					
					@Override
					public void run(JsonMapper mapper) {
						loadEntities(specFinal.entityClass, partitionFinal, entityMap, mapper);
					}
				});
			}
		}
		
		final ArrayList<Object> threadIds = new ArrayList<>();
		for (int i = 0; i < loadingThreadCount; i++) {
			final Object oneThreadId = new Object();
			threadIds.add(oneThreadId);
			new Thread() {

				final JsonMapper mapper = new JsonMapper();
				
				public void run() {
					while (true) {
						EntityLoadingTask task = null;
						synchronized (allLoadingTasks) {
							if (!allLoadingTasks.isEmpty()) {
								task = allLoadingTasks.remove(0);
							}
						}
						if (task == null) {
							break;
						}
						task.run(mapper);
					}
					
					synchronized (threadIds) {
						threadIds.remove(oneThreadId);
						threadIds.notifyAll();
					}
					System.out.println(Thread.currentThread().getName() + 
							": No more loading tasks, thread ended");
					return;
				}
			}.start();
		}
		
		synchronized (threadIds) {
			while (true) {
				if (threadIds.isEmpty()) {
					break;
				}
				try {
					threadIds.wait();
				} catch (InterruptedException e) {
				}
			}
		}
		return cachedEntities;
	}
	
	private static void loadEntities(Class<?> clazz, int partition,
			//final ArrayList<Entity> entityList, JsonMapper mapper) {
			HashMap<Long, Entity> entityMap, JsonMapper mapper) {
		
		System.out.println("Querying entity " + EntitySpec2.getShortName(clazz.getName()) + partition);
		B3ItemIterator iter = DynamoWorker.query(B3Table.Entity, EntitySpec2.getShortName(clazz.getName()) + partition);
		int counter = 0;
		while (true) {
			String json = null;
			while (true) {
				try {
					if (!iter.hasNext()) {
						break;
					}
					Item item = iter.next();
					//long id = Long.parseLong(item.getString(DynamoWorker.RANGE));
					json = item.getString(B3Table.CELL_LOCATOR_THIZ);
					break;
				} catch (ProvisionedThroughputExceededException e) {
					System.out.println(Thread.currentThread().getName() + ": " + 
							B3Table.Entity.name + ": " +  e.getMessage() + ". Sleep 1000 ms now...");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
					}
				}
			}
			
			if (json == null) {
				//no more item from table
				break;
			}
			Entity entity = (Entity) mapper.deserialize(json);
			synchronized (entityMap) {
				//entityList.add(entity);
				entityMap.put(entity.getId(), entity);
			}
			counter++;
			if (counter == 1000) {
				counter = 0;
				System.out.println(Thread.currentThread().getName() +
						": Loaded " + clazz.getName() + ": " + entityMap.size());
			}
		}
		System.out.println(Thread.currentThread().getName() +
				": Loaded " + clazz.getName() + ": " + entityMap.size());
	}
}

abstract class EntityLoadingTask {
	
	abstract void run(JsonMapper mapper);
}
