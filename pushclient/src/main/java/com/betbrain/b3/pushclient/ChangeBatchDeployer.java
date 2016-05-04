package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.model.B3ChangeBatch;
import com.betbrain.b3.model.B3Entity;

public class ChangeBatchDeployer {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	private JsonMapper mapper = new JsonMapper();
	
	private ExecutorService executor;
	
	public static void main(String[] args) {

		final int threadCount = Integer.parseInt(args[0]);
		/*if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_PUSH_WAIT)) {
			if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_PUSHING)) {
				Logger.getLogger(ChangeBatchDeployer.class).error("No bundle available for pushing");
				return;
			}
		}*/
		DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYING);
		
		//DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSHING);
		new ChangeBatchDeployer(threadCount).deployChangeBatches();
	}
	
	public ChangeBatchDeployer(int threadCount) {
		executor = Executors.newFixedThreadPool(threadCount);
	}

	public void deployChangeBatches() {

		while (true) {
			ArrayList<B3ChangeBatch> allBatches = queryForChanges();
			Collections.sort(allBatches, new Comparator<B3ChangeBatch>() {

				@Override
				public int compare(B3ChangeBatch o1, B3ChangeBatch o2) {
					return (int) (o1.batchId - o2.batchId);
				}
			});
			logger.debug("Total batches to deploy: " + allBatches.size());
			for (B3ChangeBatch oneBatch : allBatches) {
				System.out.println("Processing batch " + oneBatch.batchId);
				for (EntityChangeBase oneChange : oneBatch.changes) {
					B3Entity.applyChange(oneChange, mapper);
				}
			}
			break; //for testing only
		}
	}
	
	private ArrayList<B3ChangeBatch> queryForChanges() {
		
		LinkedList<Future<Integer>> executions = new LinkedList<Future<Integer>>(); 
		final ArrayList<B3ChangeBatch> allBatches = new ArrayList<B3ChangeBatch>();
		for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
			
			final int distFinal = dist;
			Future<Integer> oneExecution = executor.submit(new Runnable() {
				
					public void run() {
						ItemCollection<QueryOutcome> coll = DynamoWorker.query(
								B3Table.SEPC, DynamoWorker.SEPC_CHANGEBATCH + distFinal, 10);
	
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
	}
}
