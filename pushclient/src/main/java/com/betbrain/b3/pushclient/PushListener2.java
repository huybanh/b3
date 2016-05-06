package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.EntityCreate;
import com.betbrain.sepc.connector.sportsmodel.EntityDelete;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class PushListener2 implements SEPCConnectorListener, EntityChangeBatchProcessingMonitor {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	final ArrayList<EntityChangeBatch> batches = new ArrayList<EntityChangeBatch>();
	
	private long lastBatchId;
	
	private static int initialThreads;
	
	public static void main(String[] args) {
		
		if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_EMPTY)) {
			return;
		}
		
		PushListener2 listener = new PushListener2();
		initialThreads = Integer.parseInt(args[0]);
		int batchThreads = Integer.parseInt(args[1]);
		for (int i = 0; i < batchThreads; i++) {
			new Thread(new BatchWorker(listener.batches)).start();
		}
		
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYING);
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(listener);
		pushConnector.setEntityChangeBatchProcessingMonitor(listener);
		pushConnector.start("OddsHistory");
	}
	
	private int pushStatusCount = 0;

	//private int printCount;
	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		//if (printCount++ == 100) {
			//System.out.println("Got batch: " + changeBatch);
			//printCount = 0;
			//logger.info("Got batch: " + changeBatch);
		//}
		synchronized (batches) {
			batches.add(changeBatch);
			batches.notifyAll();
			lastBatchId = changeBatch.getId();
			
			if (pushStatusCount == 0) {
				DynamoWorker.updateSetting(
						new B3CellString(DynamoWorker.BUNDLE_CELL_PUSHSTATUS, DynamoWorker.BUNDLE_PUSHSTATUS_ONGOING),
						new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_RECEIVED_ID, String.valueOf(changeBatch.getId())),
						new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_RECEIVED_TIMESTAMP, changeBatch.getCreateTime().toString()));
			}
			pushStatusCount++;
			if (pushStatusCount == 1000) {
				pushStatusCount = 0;
			}
		}
	}

	public long getLastAppliedEntityChangeBatchId() {
		return lastBatchId;
	}
	
	private boolean intialDumpStarted = false;

	public void notifyInitialDump(List<? extends Entity> entityList) {
		if (intialDumpStarted) {
			logger.info("Initial dump had been already deployed. Ignored a new coming dump.");
			return;
		}
		logger.info("Starting to deploy initial dump...");
		intialDumpStarted = true;
		final HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();
		//int totalCount = 0;
		for (Entity e : entityList) {
			//totalCount++;
			HashMap<Long, Entity> subMap = masterMap.get(e.getClass().getName());
			if (subMap == null) {
				subMap = new HashMap<Long, Entity>();
				masterMap.put(e.getClass().getName(), subMap);
			}
			subMap.put(e.getId(), e);
		}
		
		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			logger.info(entry.getKey() + ": " + entry.getValue().size());
		}
		new Thread() {
			public void run() {

				new InitialDumpDeployer(masterMap, 0).initialPutMaster(initialThreads);
				DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSH_WAIT);
			}
		}.start();
	}
}

class BatchWorker implements Runnable {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	final ArrayList<EntityChangeBatch> batches;
	
	private final JsonMapper mapper = new JsonMapper();
	
	private static long printTimestamp;
	
	//private static boolean firstBatch = true;
	
	BatchWorker(ArrayList<EntityChangeBatch> batches) {
		this.batches = batches;
	}

	@Override
	public void run() {

		logger.info("Started a batch-deploying thread...");
		//int printCount = 0;
		while (true) {
			EntityChangeBatch batch;
			synchronized (batches) {
				if (batches.isEmpty()) {
					try {
						batches.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
				batch = batches.remove(0);
				if (System.currentTimeMillis() - printTimestamp > 5000) {
					printTimestamp = System.currentTimeMillis();
					logger.info(Thread.currentThread().getName() + ": Batches in queue: " + batches.size());
				}
			}

			//new change list to replace EntityChange by its wrapper (we failed serialize EntityUpdate/EntityCreate)
			LinkedList<Object> changeList = new LinkedList<Object>();
			for (EntityChange change : batch.getEntityChanges()) {
				//nameValuePairs.add(new String[] {String.valueOf(i++), serializeChange(change)});
				if (change instanceof EntityUpdate) {
					changeList.add(new EntityUpdateWrapper((EntityUpdate) change));
				} else if (change instanceof EntityCreate) {
					changeList.add(new EntityCreateWrapper((EntityCreate) change));
				} else if (change instanceof EntityDelete) {
					changeList.add(new EntityDeleteWrapper((EntityDelete) change));
				} else {
					throw new RuntimeException("Unknown change class: " + change.getClass().getName());
				}
			}
			
			//put
			String rangeKey = String.valueOf(batch.getId());
			String hashKey = generateHashKey(batch.getId());
			DynamoWorker.putSepc(hashKey, rangeKey,
				new String[] {DynamoWorker.SEPC_CELLNAME_CREATETIME, mapper.serialize(batch.getCreateTime())},
				new String[] {DynamoWorker.SEPC_CELLNAME_CHANGES, mapper.serialize(changeList)});
			
		}
	}
	
	static String generateHashKey(long batchId) {
		return DynamoWorker.SEPC_CHANGEBATCH + Math.abs(String.valueOf(batchId).hashCode() % B3Table.DIST_FACTOR);
	}
}