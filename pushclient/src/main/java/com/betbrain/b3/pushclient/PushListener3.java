package com.betbrain.b3.pushclient;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class PushListener3 implements SEPCConnectorListener, EntityChangeBatchProcessingMonitor {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	private final HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<>();
	
	private final ChangeSet[] workingChangeSet = new ChangeSet[] {new ChangeSet()};
	
	private final JsonMapper mapper = new JsonMapper();
	
	private final int initialThreads;
	
	private long lastBatchId;
	
	private int pushStatusCount = 0;
	
	private BufferedWriter sepcWriter;
	
	public static void main(String[] args) throws IOException {
		
		int initialThreadCount = Integer.parseInt(args[0]);
		
		if (!DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_EMPTY)) {
			return;
		}
		
		PushListener3 listener = new PushListener3(initialThreadCount);
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYING);
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(listener);
		pushConnector.setEntityChangeBatchProcessingMonitor(listener);
		pushConnector.start("OddsHistory");
	}
	
	private PushListener3(int initialThreadCount) throws IOException {
		this.initialThreads = initialThreadCount;
		sepcWriter = new BufferedWriter(new FileWriter("sepc", false));
	}

	@Override
	public long getLastAppliedEntityChangeBatchId() {
		return lastBatchId;
	}
	
	@Override
	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {

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

		System.out.println(Thread.currentThread().getName() + ": Processing changebatch " + changeBatch.getId());
		long changeTime = changeBatch.getCreateTime().getTime();
		for (EntityChange change : changeBatch.getEntityChanges()) {
			
			System.out.println(Thread.currentThread().getName() + ": Processing change " + change);
			
			try {
				sepcWriter.write(change.toString());
				sepcWriter.newLine();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			EntitySpec2 entitySpec = EntitySpec2.get(change.getEntityClass().getName());
			if (entitySpec == null) {
				System.out.println("Ignored unconfigured change handler " + change);
				return;
			}
			
			B3Entity<?> b3entity;
			try {
				b3entity = entitySpec.b3class.newInstance();
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			
			synchronized (workingChangeSet) {
				b3entity.applyChange(workingChangeSet[0], change, changeTime, masterMap, mapper);
				workingChangeSet[0].record(changeBatch.getId(), changeBatch.getCreateTime());
			}
		}
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

		logger.info("Starting initial deploying thread");
		new Thread() {
			public void run() {

				logger.info("Saving initial_dump file for debugging purpose");
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
				}
				
				DynamoWorker.openLocalWriters();
				new InitialDumpDeployer(immutableMasterMap, 0).initialPutMaster();
				DynamoWorker.putAllFromLocal(initialThreads);
				
				logger.info("Start deploying changesets now");
				//DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSH_WAIT);
				DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSHING);

				while (true) {
					ChangeSet changeSetToPersist;
					synchronized (workingChangeSet) {
						changeSetToPersist = workingChangeSet[0];
						workingChangeSet[0] = new ChangeSet();
					}
					changeSetToPersist.persist();
				}
			}
		}.start();
	}
}