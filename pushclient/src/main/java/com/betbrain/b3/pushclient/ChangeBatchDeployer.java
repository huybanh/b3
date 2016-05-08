package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public class ChangeBatchDeployer {
	
    //private final Logger logger = Logger.getLogger(this.getClass());
	
	//private JsonMapper mapper = new JsonMapper();
	
	//private ExecutorService executor;

	private final ArrayList<?>[] allEntityLists = new ArrayList<?>[EntitySpec2.values().length];
	
	public static void main(String[] args) {

		//final int threadCount = Integer.parseInt(args[0]);
		if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_PUSH_WAIT)) {
			if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_PUSHING)) {
				Logger.getLogger(ChangeBatchDeployer.class).error("No bundle available for pushing");
				return;
			}
		}
		//DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYING); //for testing only

		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSHING);
		ChangeBatchDeployer deployer = new ChangeBatchDeployer();
		deployer.loadEntities(Integer.parseInt(args[0]));
		deployer.deployChangeBatches();
	}
	
	private ChangeBatchDeployer() {
		//executor = Executors.newFixedThreadPool(threadCount);
	}

	private void deployChangeBatches() {
		JsonMapper mapper = new JsonMapper();
		int deployCount = 0;
		while (true) {
			final ArrayList<String> allRangeIds = new ArrayList<String>();
			for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
				ItemCollection<QueryOutcome> coll = DynamoWorker.query(
						B3Table.SEPC, DynamoWorker.SEPC_CHANGEBATCH + dist, 1);
				IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
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
					String createTime = item.getString(DynamoWorker.SEPC_CELLNAME_CREATETIME);
					String changesJson = item.getString(DynamoWorker.SEPC_CELLNAME_JSON);
					EntityChangeBase oneChange = (EntityChangeBase) mapper.deserialize(changesJson);
					B3Entity.applyChange(createTime, (EntityChangeBase) oneChange, mapper);
					DynamoWorker.delete(B3Table.SEPC, hashKey, item.getString(DynamoWorker.RANGE));
					if (deployCount == 0) {
						Date d = new Date(Long.parseLong(createTime));
						DynamoWorker.updateSetting(
								new B3CellString(DynamoWorker.BUNDLE_CELL_DEPLOYSTATUS, DynamoWorker.BUNDLE_PUSHSTATUS_ONGOING),
								new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_ID, String.valueOf(batchId)),
								new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_TIMESTAMP, d.toString()));
					}
					deployCount++;
					if (deployCount == 1000) {
						deployCount = 0;
					}
				}
				batchId++;
			}
		}
	}
	
	private void loadEntities(int loadingThreadCount) {
		
		for (int i = 0; i < allEntityLists.length; i++) {
			allEntityLists[i] = new ArrayList<>();
		}
		final ArrayList<EntityLoadingTask> allLoadingTasks = new ArrayList<>();
		for (EntitySpec2 spec : EntitySpec2.values()) {
			final EntitySpec2 specFinal = spec;
			//final ArrayList<Entity> entityList = new ArrayList<>();
			for (int partition = 0; partition < B3Table.DIST_FACTOR; partition++) {
				final int partitionFinal = partition;
				allLoadingTasks.add(new EntityLoadingTask() {
					
					@SuppressWarnings("unchecked")
					@Override
					public void run(JsonMapper mapper) {
						loadEntities(specFinal.entityClass, partitionFinal, 
								(ArrayList<Entity>) allEntityLists[specFinal.ordinal()], mapper);
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
	}
	
	private static void loadEntities(Class<?> clazz, int partition,
			final ArrayList<Entity> entityList, JsonMapper mapper) {
		
		ItemCollection<QueryOutcome> coll = DynamoWorker.query(
				B3Table.Entity, EntitySpec2.getShortName(clazz.getName()) + partition);
		IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
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
			synchronized (entityList) {
				entityList.add(entity);
			}
		}
	}
	
	/*private ArrayList<B3ChangeBatch> queryForChanges() {
		
		LinkedList<Future<Integer>> executions = new LinkedList<Future<Integer>>(); 
		final ArrayList<B3ChangeBatch> allBatches = new ArrayList<B3ChangeBatch>();
		for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
			
			final int distFinal = dist;
			Future<Integer> oneExecution = executor.submit(new Runnable() {
				
					public void run() {
						ItemCollection<QueryOutcome> coll = DynamoWorker.query(
								B3Table.SEPC, DynamoWorker.SEPC_CHANGEBATCH + distFinal, 1);
	
						IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
						//int changeBatchCount = 0;
						while (iter.hasNext()) {
							Item item = iter.next();
							//String batchId = item.getString(DynamoWorker.RANGE);
							//String createTime = item.getString(DynamoWorker.SEPC_CELLNAME_CREATETIME);
							String changesJson = item.getString(DynamoWorker.SEPC_CELLNAME_CHANGES);
							//System.out.println(changesJson);
							@SuppressWarnings("unchecked")
							List<Object> changes = (List<Object>) mapper.deserialize(changesJson);
							B3ChangeBatch batch = new B3ChangeBatch(item.getLong(DynamoWorker.RANGE));
							allBatches.add(batch);
							for (Object obj : changes) {
								if (obj != null) {
									batch.changes.add((EntityChangeBase) obj);
								} else {
									System.out.println("NULL CHANGE: " + changesJson);
									Thread.dumpStack();
								}
							}
							//System.out.println(changes);
							//changeBatchCount++;
							//if (changeBatchCount % 2 == 0) {
							//	System.out.println("Change-batch count: " + changeBatchCount);
								//break;
							//}
						}
					}
				}, 1);
			executions.add(oneExecution);
		}
		
		for (Future<Integer> one : executions) {
			while (true) {
				try {
					one.get();
				} catch (InterruptedException e) {
					continue;
				} catch (ExecutionException e) {
					continue;
				}
				break;
			}
		}
		return allBatches;
	}*/
}

abstract class EntityLoadingTask {
	
	abstract void run(JsonMapper mapper);
}
