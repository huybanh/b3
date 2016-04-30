package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;
import com.betbrain.b3.data.B3Bundle;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class PushListener implements SEPCConnectorListener, EntityChangeBatchProcessingMonitor {
	
	private B3Bundle bundle;
	
	final ArrayList<EntityChangeBatch> batches = new ArrayList<EntityChangeBatch>();
	
	private long lastBatchId;
	
	private final Object initialListLock = new Object();
	
	private boolean started = false;
	
	private int initialThreads;
	
	public static void main(String[] args) {
		
		DynamoWorker.initialize();
		ModelShortName.initialize();
		
		PushListener listener = new PushListener();
		listener.initialThreads = Integer.parseInt(args[0]);
		int batchThreads = Integer.parseInt(args[1]);
		
		listener.bundle = DynamoWorker.getBundleUnused(DynamoWorker.BUNDLE_STATUS_INITIALPUT);
		System.out.println("Working bundle: " + listener.bundle);
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(listener);
		pushConnector.setEntityChangeBatchProcessingMonitor(listener);
		pushConnector.start("OddsHistory");
		for (int i = 0; i < batchThreads; i++) {
			new Thread(new BatchWorker(listener.bundle, listener.batches)).start();
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
		synchronized (initialListLock) {
			if (started) {
				return;
			}
			started = true;
		}
		for (int i = 0; i < initialThreads; i++) {
			new Thread(new InitialWorker(bundle, initialListLock, entityList)).start();
		}
	}
}

class BatchWorker implements Runnable {
	
	final ArrayList<EntityChangeBatch> batches;
	
	private final B3Bundle bundle;
	
	private final JsonMapper mapper = new JsonMapper();
	
	BatchWorker(B3Bundle bundle, ArrayList<EntityChangeBatch> batches) {
		this.bundle = bundle;
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

			//new change list to replace EntityUpdate by EntityUpdateWrapper (we failed serialize EntityUpdate)
			LinkedList<Object> changeList = new LinkedList<Object>();
			for (EntityChange change : batch.getEntityChanges()) {
				//nameValuePairs.add(new String[] {String.valueOf(i++), serializeChange(change)});
				if (change instanceof EntityUpdate) {
					changeList.add(new EntityUpdateWrapper((EntityUpdate) change));
				} else {
					changeList.add(change);
				}
			}
			
			//put
			String rangeKey = String.valueOf(batch.getId());
			String hashKey = DynamoWorker.SEPC_CHANGEBATCH + Math.abs(rangeKey.hashCode() % B3Table.DIST_FACTOR);
			//DynamoWorker.putSepc(bundle, hashKey, "BATCH", mapper.serialize(batch));
			DynamoWorker.putSepc(bundle, hashKey, rangeKey,
				new String[] {DynamoWorker.SEPC_CELLNAME_CREATETIME, mapper.serialize(batch.getCreateTime())},
				new String[] {DynamoWorker.SEPC_CELLNAME_CHANGES, mapper.serialize(changeList)});
		}
	}
}

class InitialWorker implements Runnable {
	
	private final Object initialListLock;
	
	private final List<? extends Entity> initialList;
	
	private final B3Bundle bundle;
	
	private final JsonMapper mapper = new JsonMapper();
	
	public InitialWorker(B3Bundle bundle, Object initialListLock, List<? extends Entity> initialList) {
		this.bundle = bundle;
		this.initialListLock = initialListLock;
		this.initialList = initialList;
	}
	
	private static final int prefixLength = "com.betbrain.sepc.connector.".length();

	public void run() {
		int printCount = 0;
		while (true) {
			Entity entity;
			synchronized (initialListLock) {
				if (initialList.isEmpty()) {
					DynamoWorker.setBundleStatus(bundle, DynamoWorker.BUNDLE_STATUS_DEPLOYWAIT);
					return;
				}
				entity = initialList.remove(0);
				if (printCount++ == 1000) {
					printCount = 0;
					System.out.println("Initial remains: " + initialList.size());
				}
			}
			
			String rangeKey = entity.getClass().getName().substring(prefixLength) + "/" + entity.getId();
			String hashKey = DynamoWorker.SEPC_INITIAL + Math.abs(rangeKey.hashCode() % B3Table.DIST_FACTOR);
			String json = mapper.serialize(entity);
			String[] nameValue = new String[] {DynamoWorker.SEPC_CELLNAME_JSON, json};
			DynamoWorker.putSepc(bundle, hashKey, rangeKey, nameValue);
		}
	}
}