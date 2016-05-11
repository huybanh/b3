package com.betbrain.b3.pushclient;

import java.io.IOException;
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
import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ChangeBase;
import com.betbrain.b3.data.ChangeCreateWrapper;
import com.betbrain.b3.data.ChangeDeleteWrapper;
import com.betbrain.b3.data.ChangeUpdateWrapper;
import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class PushListener2 implements SEPCConnectorListener, EntityChangeBatchProcessingMonitor {
	
    private final Logger logger = Logger.getLogger(this.getClass());

	final LinkedList<EntityChangeBatch> batches = new LinkedList<EntityChangeBatch>();
	
	private long lastBatchId;
	
	private static int initialThreads;
	
	public static void main(String[] args) throws IOException {
		
		BatchWorkerFile.init();
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
		/*try {
			BatchWorkerFile.save(changeBatch);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}*/
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
				DynamoWorker.openLocalWriters();
				new InitialDumpDeployer(masterMap, 0).initialPutMaster();
				DynamoWorker.putAllFromLocal(initialThreads);
				DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSH_WAIT);
			}
		}.start();
	}
}

class BatchWorker implements Runnable {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	//final ArrayList<EntityChangeBatch> batches;
    final LinkedList<EntityChangeBatch> batches;
	
	private final JsonMapper mapper = new JsonMapper();
	
	private static long printTimestamp;
	
	//private static boolean firstBatch = true;
	
	static final int BATCHID_DIGIT_COUNT = "00026473973523".length(); //sample batch id: 26473973523
	
	BatchWorker(LinkedList<EntityChangeBatch> batches) {
		this.batches = batches;
	}

	@Override
	public void run() {

		logger.info("Started a batch-deploying thread...");
		//int printCount = 0;
		while (true) {
			EntityChangeBatch batch;
			long nextBatchId;
			synchronized (batches) {
				if (batches.isEmpty()) {
				//if (batches.size() < 2) { //performance error: linked list
					try {
						batches.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
				batch = batches.removeFirst();
				if (System.currentTimeMillis() - printTimestamp > 5000) {
					printTimestamp = System.currentTimeMillis();
					logger.info(Thread.currentThread().getName() + ": Batches in queue: " + batches.size());
				}
				
				while (true) {
					if (batches.isEmpty()) {
						//if (batches.size() < 2) { //performance error: linked list
						try {
							batches.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}
					nextBatchId = batches.getFirst().getId();
					break;
				}
			}

			//new change list to replace EntityChange by its wrapper (we failed serialize EntityUpdate/EntityCreate)
			//LinkedList<Object> changeList = new LinkedList<Object>();
			int changeIndex = 0;
			int batchDigitCount = String.valueOf(batch.getEntityChanges().size()).length();
			for (EntityChange change : batch.getEntityChanges()) {
				//nameValuePairs.add(new String[] {String.valueOf(i++), serializeChange(change)});
				ChangeBase wrapper;
				if (change instanceof EntityUpdate) {
					wrapper = new ChangeUpdateWrapper((EntityUpdate) change);
					String error = ((ChangeUpdateWrapper) wrapper).validate();
					if (error != null) {
						DynamoWorker.logError(error);
						continue;
					}
				} else if (change instanceof EntityCreate) {
					wrapper = new ChangeCreateWrapper((EntityCreate) change);
				} else if (change instanceof EntityDelete) {
					wrapper = new ChangeDeleteWrapper((EntityDelete) change);
				} else {
					throw new RuntimeException("Unknown change class: " + change.getClass().getName());
				}
				String hashKey = generateChangeBatchHashKey(batch.getId());
				String rangeKey = B3Key.zeroPadding(BATCHID_DIGIT_COUNT, batch.getId()) +
						B3Table.KEY_SEP + B3Key.zeroPadding(batchDigitCount, changeIndex);
				changeIndex++;
				DynamoWorker.put(true, B3Table.SEPC, hashKey, rangeKey,
					new B3CellString(DynamoWorker.SEPC_CELLNAME_NEXTBATCH, String.valueOf(nextBatchId)),
					new B3CellString(DynamoWorker.SEPC_CELLNAME_CREATETIME, mapper.serialize(batch.getCreateTime())),
					new B3CellString(DynamoWorker.SEPC_CELLNAME_JSON, mapper.serialize(wrapper)));
			}
			
		}
	}
	
	static String generateChangeBatchHashKey(long batchId) {
		return DynamoWorker.SEPC_CHANGEBATCH + Math.abs(String.valueOf(batchId).hashCode() % B3Table.DIST_FACTOR);
	}
}