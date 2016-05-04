package com.betbrain.b3.pushclient;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;

import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.ChangeBatchDeployer;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;

public class PushDeployer {
	
	final LinkedList<EntityChangeBatch> batches = new LinkedList<EntityChangeBatch>();
	
	public static void main(String[] args) {

		final int threadCount = Integer.parseInt(args[0]);
		DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYWAIT);
		//DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYING); //TESTING
		final JsonMapper mapper = new JsonMapper();
		
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYING);
		final HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();
		
		final LinkedList<Runnable> initialDumpLoadTasks = new LinkedList<Runnable>();
		for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
			final int distFinal = dist;
			initialDumpLoadTasks.add(new Runnable() {
				public void run() {
					ItemCollection<QueryOutcome> coll = DynamoWorker.query(
							B3Table.SEPC, DynamoWorker.SEPC_INITIAL + distFinal);

					IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
					int itemCount = 0;
					while (iter.hasNext()) {
						Item item = iter.next();
						String json = item.getString(DynamoWorker.SEPC_CELLNAME_JSON);
						Entity entity = mapper.deserializeEntity(json);
						synchronized (masterMap) {
							HashMap<Long, Entity> subMap = masterMap.get(entity.getClass().getName());
							if (subMap == null) {
								subMap = new HashMap<Long, Entity>();
								masterMap.put(entity.getClass().getName(), subMap);
							}
							subMap.put(entity.getId(), entity);
						}
						itemCount++;
						//System.out.println("Entity " + itemCount + ": " + entity);
						if (itemCount % 10000 == 0) {
							System.out.println("Read count: " + itemCount);
						}
					}
				}
			});
		}
		
		//start initial-dump loading threads
		final LinkedList<Object> threadIds = new LinkedList<Object>();
		for (int i = 0; i < threadCount; i++) {
			final Object threadId = new Object();
			threadIds.add(threadId);
			new Thread() {
				public void run() {
					while (true) {
						Runnable oneRunner;
						synchronized (initialDumpLoadTasks) {
							System.out.println("Remaining runners: " + initialDumpLoadTasks.size());
							if (initialDumpLoadTasks.isEmpty()) {
								threadIds.remove(threadId);
								initialDumpLoadTasks.notifyAll();
								return;
							}
							oneRunner = initialDumpLoadTasks.remove();
						}
						oneRunner.run();
					}
				}
			}.start();
		}
		
		//wait for all initial-dump loading threads to finish
		while (true) {
			synchronized (initialDumpLoadTasks) {
				if (threadIds.isEmpty()) {
					break;
				}
				try {
					initialDumpLoadTasks.wait();
				} catch (InterruptedException e) {
				}
			}
		}
		
		//start initial-dump deploying threads
		/*Runnable initialDumpDeployTask = new Runnable() {
			
			public void run() {
				for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
					System.out.println(entry.getKey() + ": " + entry.getValue().size());
				}
				new InitialDumpDeployer(masterMap).initialPutMaster(threadCount);
			}
		};
		initialDumpDeployTask.run();*/
		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue().size());
		}
		//new InitialDumpDeployer(masterMap).initialPutMaster(threadCount);
		
		//all initial-dump deploying threads have finished
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSHING);
		new ChangeBatchDeployer().deployChangeBatches();
	}

}
