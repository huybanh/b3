package com.betbrain.b3.pushclient;

import java.util.ArrayList;
import java.util.List;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

@Deprecated
public class PushListener implements SEPCConnectorListener, EntityChangeBatchProcessingMonitor {
	
	final ArrayList<EntityChangeBatch> batches = new ArrayList<EntityChangeBatch>();
	
	private long lastBatchId;
	
	private final Object initialListLock = new Object();
	
	private boolean started = false;
	
	private int initialThreads;
	
	public static void main(String[] args) {
		
		DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_EMPTY);
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_INITIALPUT);
		
		PushListener listener = new PushListener();
		listener.initialThreads = Integer.parseInt(args[0]);
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
		synchronized (initialListLock) {
			if (started) {
				return;
			}
			started = true;
		}
		for (int i = 0; i < initialThreads; i++) {
			new Thread(new InitialWorker(initialListLock, entityList)).start();
		}
	}
}

class InitialWorker implements Runnable {
	
	private final Object initialListLock;
	
	private final List<? extends Entity> initialList;
	
	private final JsonMapper mapper = new JsonMapper();
	
	public InitialWorker(Object initialListLock, List<? extends Entity> initialList) {
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
					DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYWAIT);
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
			//B3CellString[] nameValue = new B3CellString[] {new B3CellString(DynamoWorker.SEPC_CELLNAME_JSON, json)};
			DynamoWorker.put(true, B3Table.SEPC, hashKey, rangeKey, new B3CellString(DynamoWorker.SEPC_CELLNAME_JSON, json));
		}
	}
}