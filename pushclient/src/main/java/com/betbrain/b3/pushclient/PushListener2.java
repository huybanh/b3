package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.EntityCreate;
import com.betbrain.sepc.connector.sportsmodel.EntityDelete;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class PushListener2 implements SEPCConnectorListener, EntityChangeBatchProcessingMonitor {
	
	final ArrayList<EntityChangeBatch> batches = new ArrayList<EntityChangeBatch>();
	
	private long lastBatchId;
	
	private static int initialThreads;
	
	public static void main(String[] args) {
		
		DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_EMPTY);
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_INITIALPUT);
		
		PushListener2 listener = new PushListener2();
		initialThreads = Integer.parseInt(args[0]);
		int batchThreads = Integer.parseInt(args[1]);
		
		//System.out.println("Working bundle: " + listener.bundle);
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(listener);
		pushConnector.setEntityChangeBatchProcessingMonitor(listener);
		pushConnector.start("OddsHistory");
		for (int i = 0; i < batchThreads; i++) {
			new Thread(new BatchWorker(listener.batches)).start();
		}
	}

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		synchronized (batches) {
			batches.add(changeBatch);
			batches.notifyAll();
			lastBatchId = changeBatch.getId();
		}
	}

	public long getLastAppliedEntityChangeBatchId() {
		return lastBatchId;
	}

	public void notifyInitialDump(List<? extends Entity> entityList) {
		
		final HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();
		for (Entity e : entityList) {
			HashMap<Long, Entity> subMap = masterMap.get(e.getClass().getName());
			if (subMap == null) {
				subMap = new HashMap<Long, Entity>();
				masterMap.put(e.getClass().getName(), subMap);
			}
			subMap.put(e.getId(), e);
		}
		
		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue().size());
		}
		new InitialDumpDeployer(masterMap).initialPutMaster(initialThreads);
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYWAIT);
	}
}

class BatchWorker implements Runnable {
	
	final ArrayList<EntityChangeBatch> batches;
	
	private final JsonMapper mapper = new JsonMapper();
	
	BatchWorker(ArrayList<EntityChangeBatch> batches) {
		this.batches = batches;
	}

	public void run() {
		
		int printCount = 0;
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
				if (printCount++ == 1000) {
					printCount = 0;
					System.out.println("Batches: " + batches.size());
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
			String hashKey = DynamoWorker.SEPC_CHANGEBATCH + Math.abs(rangeKey.hashCode() % B3Table.DIST_FACTOR);
			//DynamoWorker.putSepc(hashKey, "BATCH", mapper.serialize(batch));
			DynamoWorker.putSepc(hashKey, rangeKey,
				new String[] {DynamoWorker.SEPC_CELLNAME_CREATETIME, mapper.serialize(batch.getCreateTime())},
				new String[] {DynamoWorker.SEPC_CELLNAME_CHANGES, mapper.serialize(changeList)});
		}
	}
}