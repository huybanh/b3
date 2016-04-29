package com.betbrain.b3.pushclient;

import java.util.LinkedList;
import java.util.List;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.b3.data.B3Bundle;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class PushListener implements SEPCConnectorListener {
	
	private B3Bundle bundle;
	
	final LinkedList<EntityChangeBatch> batches = new LinkedList<EntityChangeBatch>();
	
	private int initialThreads;
	
	public static void main(String[] args) {
		
		DynamoWorker.initialize();
		ModelShortName.initialize();
		
		PushListener listener = new PushListener();
		listener.initialThreads = Integer.parseInt(args[0]);
		int batchThreads = Integer.parseInt(args[1]);
		
		listener.bundle = DynamoWorker.getBundleUnused(DynamoWorker.BUNDLE_STATUS_INITIALPUT);
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(listener);
		//pushConnector.setEntityChangeBatchProcessingMonitor(new BatchMonitor());
		pushConnector.start("OddsHistory");
		for (int i = 0; i < batchThreads; i++) {
			new Thread(new BatchWorker(listener.bundle, listener.batches)).start();
		}
	}

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		synchronized (batches) {
			batches.add(changeBatch);
			batches.notifyAll();
		}
	}

	public void notifyInitialDump(List<? extends Entity> entityList) {

		final Object initialListLock = new Object();
		for (int i = 0; i < initialThreads; i++) {
			new Thread(new InitialWorker(bundle, initialListLock, entityList)).start();
		}
	}
}

class BatchWorker implements Runnable {
	
	final LinkedList<EntityChangeBatch> batches;
	
	private final B3Bundle bundle;
	
	private final JsonMapper mapper = new JsonMapper();
	
	BatchWorker(B3Bundle bundle, LinkedList<EntityChangeBatch> batches) {
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
				batch = batches.remove();
				if (printCount++ == 100) {
					printCount = 0;
					System.out.println("Batches: " + batches.size());
				}
			}

			final String hashKey = DynamoWorker.SEPC_CHANGEBATCH + batch.getId();
			DynamoWorker.putSepc(bundle, hashKey, "BATCH", mapper.serialize(batch));
			int i = 0;
			for (EntityChange change : batch.getEntityChanges()) {
				DynamoWorker.putSepc(bundle, hashKey, String.valueOf(i++), mapper.serialize(change));
			}
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
			DynamoWorker.putSepc(bundle, 
					DynamoWorker.SEPC_INITIAL + entity.getClass().getName().substring(prefixLength) + "/" + entity.getId(),
					DynamoWorker.SEPC_CELLNAME, mapper.serialize(entity));
		}
	}
}