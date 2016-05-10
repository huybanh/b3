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
import com.betbrain.sepc.connector.sportsmodel.EntityCreate;
import com.betbrain.sepc.connector.sportsmodel.EntityDelete;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.data.ChangeBase;
import com.betbrain.b3.data.ChangeCreateWrapper;
import com.betbrain.b3.data.ChangeDeleteWrapper;
import com.betbrain.b3.data.ChangeSet;
import com.betbrain.b3.data.ChangeUpdateWrapper;
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
	
	public static void main(String[] args) throws IOException {
		
		int initialThreadCount = Integer.parseInt(args[0]);
		BatchWorkerFile.init();
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
	
	private PushListener3(int initialThreadCount) {
		
		this.initialThreads = initialThreadCount;

		new Thread() {
			public void run() {
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

	@Override
	public long getLastAppliedEntityChangeBatchId() {
		return lastBatchId;
	}
	
	@Override
	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		try {
			BatchWorkerFile.save(changeBatch);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

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

		System.out.println(Thread.currentThread().getName() + ": Processing change " + changeBatch.getId());
		for (EntityChange change : changeBatch.getEntityChanges()) {
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
		
			EntitySpec2 entitySpec = EntitySpec2.get(wrapper.getEntityClassName());
			if (entitySpec == null) {
				//System.out.println("Ignoring unconfigured change handler " + change);
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
				b3entity.applyChange(workingChangeSet[0], wrapper, masterMap, mapper);
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
		logger.info("Starting to deploy initial dump...");
		intialDumpStarted = true;

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
				new InitialDumpDeployer(masterMap, 0).initialPutMaster();
				DynamoWorker.putAllFromLocal(initialThreads);
				//DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSH_WAIT);
				DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSHING);
			}
		}.start();
	}
}