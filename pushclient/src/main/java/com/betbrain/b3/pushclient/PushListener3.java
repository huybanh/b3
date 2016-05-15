package com.betbrain.b3.pushclient;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.data.ChangeSet;
import com.betbrain.b3.data.ChangeSetItem;
import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class PushListener3 implements SEPCConnectorListener, EntityChangeBatchProcessingMonitor {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	private final HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<>();
	
	private final ChangeSet[] changesetWorking = new ChangeSet[] {new ChangeSet()};
	
	private ChangeSet changesetPersiting = null;
	
	private final Object changesetPersitingLock = new Object();
	
	private static int initialThreadCount;
	
	private static int pushThreadCount;
	
	//private long lastBatchId;
	private EntityChangeBatch lastBatch;
	
	//private int pushStatusCount = 0;
	
	//private BufferedWriter sepcWriter;
	
	public static void main(String[] args) throws IOException {
		
		initialThreadCount = Integer.parseInt(args[0]);
		pushThreadCount = Integer.parseInt(args[1]);
		
		//DynamoWorker.initBundleCurrent();
		if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_EMPTY)) {
			return;
		}
		
		PushListener3 listener = new PushListener3();
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYING);
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(listener);
		pushConnector.setEntityChangeBatchProcessingMonitor(listener);
		pushConnector.start("OddsHistory");
	}
	
	private PushListener3() throws IOException {
		//sepcWriter = new BufferedWriter(new FileWriter("sepc", false));
	}

	@Override
	public long getLastAppliedEntityChangeBatchId() {
		if (lastBatch == null) {
			return 0;
		}
		return lastBatch.getId();
	}
	
	private boolean intialDumpStarted = false;

	@Override
	public void notifyInitialDump(final List<? extends Entity> entityList) {
		if (intialDumpStarted) {
			logger.info("Initial dump had been already deployed. Ignored a new coming dump.");
			return;
		}
		intialDumpStarted = true;

		for (Entity e : entityList) {
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

		logger.info("Starting initial deploying thread");
		new Thread() {
			public void run() {
				processInitialDump(entityList);
			}
		}.start();
	}
	
	private void processInitialDump(List<? extends Entity> entityList) {
		
		/*logger.info("Saving initial_dump file for debugging purpose");
		JsonMapper jsonMapper = new JsonMapper();
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("initial_dump", false));
			for (Entity e : entityList) {
				writer.write((jsonMapper.serialize(e)));
				writer.newLine();
			}
			writer.close();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}*/
		
		//another map for initial dump
		final HashMap<String, HashMap<Long, Entity>> immutableMasterMap = new HashMap<>();
		for (Entity e : entityList) {
			HashMap<Long, Entity> subMap = immutableMasterMap.get(e.getClass().getName());
			if (subMap == null) {
				subMap = new HashMap<Long, Entity>();
				immutableMasterMap.put(e.getClass().getName(), subMap);
			}
			subMap.put(e.getId(), e);
		}
		DynamoWorker.openLocalWriters();
		new InitialDumpDeployer(immutableMasterMap, 0).initialPutMaster();
		logger.info("Start deploying initial dump");
		DynamoWorker.putAllFromLocal(initialThreadCount);
		
		logger.info("Start deploying changesets now");
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSHING);

		for (int i = 0; i < pushThreadCount; i++) {
			new Thread("Push-thread-" + i) {
				public void run() {
					persistChanges();
				}
			}.start();
		}
	}
	
	private final JsonMapper pushingMapper = new JsonMapper();
	
	@Override
	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {

		/*if (pushStatusCount == 0) {
			DynamoWorker.updateSetting(
					new B3CellString(DynamoWorker.BUNDLE_CELL_PUSHSTATUS, DynamoWorker.BUNDLE_PUSHSTATUS_ONGOING),
					new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_RECEIVED_ID, String.valueOf(changeBatch.getId())),
					new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_RECEIVED_TIMESTAMP, changeBatch.getCreateTime().toString()));
		}
		pushStatusCount++;
		if (pushStatusCount == 1000) {
			pushStatusCount = 0;
		}*/

		//System.out.println(Thread.currentThread().getName() + ": Processing changebatch " + changeBatch.getId());
		long changeTime = changeBatch.getCreateTime().getTime();
		for (EntityChange change : changeBatch.getEntityChanges()) {
			
			//System.out.println(Thread.currentThread().getName() + ": Processing change " + change);
			/*try {
				sepcWriter.write(changeBatch.getId() + ":" + changeBatch.getCreateTime() + ":" + change.toString());
				sepcWriter.newLine();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}*/
			
			EntitySpec2 entitySpec = EntitySpec2.get(change.getEntityClass().getName());
			if (entitySpec == null) {
				//System.out.println("Ignored unconfigured change handler " + change);
				continue;
			}
			
			B3Entity<?> b3entity;
			try {
				b3entity = entitySpec.b3class.newInstance();
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			
			synchronized (changesetWorking) {
				b3entity.applyChange(changesetWorking[0], change, changeTime, masterMap, pushingMapper);
				changesetWorking[0].record(changeBatch.getId(), changeBatch.getCreateTime());
			}
		}
		lastBatch = changeBatch;
	}
	
	private int statusUpdateCount;
	
	private void persistChanges() {
		
		int persistedCount = 0;
		while (true) {
			
			ChangeSetItem oneChange;
			synchronized (changesetPersitingLock) {
				
				if (changesetPersiting != null) {
					oneChange = changesetPersiting.checkout();
				} else {
					oneChange = null;
				}
				
				if (oneChange == null) {
					
					//change to next changeset
					synchronized (changesetWorking) {
						changesetWorking[0].close();
						changesetPersiting = changesetWorking[0];
						changesetWorking[0] = new ChangeSet();
					}
					
					//update status
					Date lastBatchTime = changesetPersiting.getLastBatchTime();
					if (lastBatchTime == null) {
						//this changeset has no changes
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
						}
						continue;
					}
					
					//update status to setting table
					if (statusUpdateCount == 0) {
						if (lastBatch != null) {
							DynamoWorker.updateSetting(
									new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_RECEIVED_ID, String.valueOf(lastBatch.getId())),
									new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_RECEIVED_TIMESTAMP, lastBatch.getCreateTime().toString()),
									new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_ID, String.valueOf(changesetPersiting.getLastBatchId())),
									new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_TIMESTAMP, lastBatchTime.toString()));
						} else {
							DynamoWorker.updateSetting(
									new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_ID, String.valueOf(changesetPersiting.getLastBatchId())),
									new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_TIMESTAMP, lastBatchTime.toString()));
							
						}
					}
					statusUpdateCount++;
					if (statusUpdateCount == 1) {
						statusUpdateCount = 0;
					}
					continue;
				}
				
				//got one non-null change
				//logging
				if (persistedCount == 1000) {
					persistedCount = 0;
					System.out.println(Thread.currentThread().getName() + 
							": remain updates to persist: " + changesetPersiting.countChangesBeingPersisted());
				}
				persistedCount++;
			}
			
			oneChange.persist();
		}
	}
}