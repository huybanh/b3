package com.betbrain.b3.pushclient;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.b3.data.B3Bundle;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.InitialPutHandler;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;

public class PushDeployer {
	
	final LinkedList<EntityChangeBatch> batches = new LinkedList<EntityChangeBatch>();
	
	public static void main(String[] args) {

		final int threadCount = Integer.parseInt(args[0]);
		DynamoWorker.initialize();
		ModelShortName.initialize();
		final JsonMapper mapper = new JsonMapper();
		
		final B3Bundle bundle = DynamoWorker.getBundleByStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYWAIT);
		if (bundle == null) {
			System.out.println("Found no bundles for depoying");
			return;
		}
		DynamoWorker.setBundleStatus(bundle, DynamoWorker.BUNDLE_STATUS_DEPLOYING);

		final HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();		
		//final Runnable[] masterRunner = new Runnable[1];
		Runnable masterRunner = new Runnable() {
			
			public void run() {
				for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
					System.out.println(entry.getKey() + ": " + entry.getValue().size());
				}
				new InitialPutHandler(bundle, masterMap).initialPutMaster(threadCount);
			}
		};

		final LinkedList<Runnable> runners = new LinkedList<Runnable>();
		for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
			final int distFinal = dist;
			runners.add(new Runnable() {
				public void run() {
					ItemCollection<QueryOutcome> coll = DynamoWorker.query(
							bundle, B3Table.SEPC, DynamoWorker.SEPC_INITIAL + distFinal);

					IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
					int itemCount = 0;
					while (iter.hasNext()) {
						Item item = iter.next();
						String json = item.getString(DynamoWorker.SEPC_CELLNAME_JSON);
						Entity entity = mapper.deserialize(json);
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
		
		final LinkedList<Object> threadIds = new LinkedList<Object>();
		LinkedList<Thread> threads = new LinkedList<Thread>();
		for (int i = 0; i < threadCount; i++) {
			final Object threadId = new Object();
			threadIds.add(threadId);
			threads.add(
				new Thread() {
					public void run() {
						while (true) {
							Runnable oneRunner;
							synchronized (runners) {
								System.out.println("Remaining runners: " + runners.size());
								if (runners.isEmpty()) {
									threadIds.remove(threadId);
									runners.notifyAll();
									return;
								}
								oneRunner = runners.remove();
							}
							oneRunner.run();
						}
					}
				});
		}
		for (Thread t : threads) {
			t.start();
		}
		
		while (true) {
			synchronized (runners) {
				if (threadIds.isEmpty()) {
					break;
				}
				try {
					runners.wait();
				} catch (InterruptedException e) {
				}
			}
		}
		masterRunner.run();
	}

}
