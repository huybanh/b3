package com.betbrain.b3.pushclient;

import java.util.HashMap;
import java.util.LinkedList;

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

		final int threads = Integer.parseInt(args[0]);
		DynamoWorker.initialize();
		ModelShortName.initialize();
		final JsonMapper mapper = new JsonMapper();
		
		final B3Bundle bundle = DynamoWorker.getBundleUnused(DynamoWorker.BUNDLE_STATUS_INITIALPUT);
		if (bundle == null) {
			System.out.println("Found no bundles for depoying");
			return;
		}

		final HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();		
		final Runnable[] masterRunner = new Runnable[1];
		masterRunner[0] = new Runnable() {
			
			public void run() {
				new InitialPutHandler(bundle, masterMap).initialPutMaster(threads);
			}
		};

		final LinkedList<Runnable> runners = new LinkedList<Runnable>();
		for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
			final int distFinal = dist;
			runners.add(new Runnable() {
				public void run() {
					ItemCollection<QueryOutcome> coll = DynamoWorker.query(
							bundle, B3Table.SEPC, DynamoWorker.SEPC_INITIAL + distFinal);

					int count = 0;
					IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
					while (iter.hasNext()) {
						Item item = iter.next();
						String json = item.getString(DynamoWorker.SEPC_CELLNAME_JSON);
						Entity entity = mapper.deserialize(json);
						HashMap<Long, Entity> subMap = masterMap.get(entity.getClass().getName());
						if (subMap == null) {
							subMap = new HashMap<Long, Entity>();
							masterMap.put(entity.getClass().getName(), subMap);
						}
						subMap.put(entity.getId(), entity);
						if (++count % 100000 == 0) {
							System.out.println("Read line: " + count);
						}
					}
				}
			});
		}
		
		for (int i = 0; i < threads; i++) {
			new Thread() {
				public void run() {
					Runnable oneRunner;
					synchronized (runners) {
						if (runners.isEmpty()) {
							if (masterRunner[0] == null) {
								return;
							}
							oneRunner = masterRunner[0];
							masterRunner[0] = null;
						} else {
							oneRunner = runners.remove();
						}
					}
					oneRunner.run();
				}
			}.start();
		}
	}

}
