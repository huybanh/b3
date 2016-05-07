package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.model.B3Entity;

public class ChangeBatchDeployer {
	
    //private final Logger logger = Logger.getLogger(this.getClass());
	
	private JsonMapper mapper = new JsonMapper();
	
	//private ExecutorService executor;
	
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
		new ChangeBatchDeployer().deployChangeBatches();
	}
	
	public ChangeBatchDeployer() {
		//executor = Executors.newFixedThreadPool(threadCount);
	}

	public void deployChangeBatches() {

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
				String hashKey = BatchWorker.generateHashKey(batchId);
				ItemCollection<QueryOutcome> coll = DynamoWorker.queryRangeBeginsWith(
						B3Table.SEPC, hashKey, String.valueOf(batchId));
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
				/*logger.debug("Total batches to deploy: " + allBatches.size());
				for (B3ChangeBatch oneBatch : allBatches) {
					System.out.println("Processing batch " + oneBatch.batchId);
					for (EntityChangeBase oneChange : oneBatch.changes) {
						B3Entity.applyChange(oneChange, mapper);
					}
				}*/
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
