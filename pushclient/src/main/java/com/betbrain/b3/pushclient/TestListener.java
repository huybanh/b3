package com.betbrain.b3.pushclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.data.ChangeSet;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class TestListener implements SEPCConnectorListener, EntityChangeBatchProcessingMonitor {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	private final HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<>();
	
	private final ChangeSet[] changesetWorking = new ChangeSet[] {new ChangeSet()};
	
	private long lastBatchId;
	
	public static void main(String[] args) throws IOException {
		
		EntitySpec2.initialize();
		TestListener listener = new TestListener();
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(listener);
		pushConnector.setEntityChangeBatchProcessingMonitor(listener);
		pushConnector.start("OddsHistory");
	}

	@Override
	public long getLastAppliedEntityChangeBatchId() {
		return lastBatchId;
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
	}
	
	private final JsonMapper pushingMapper = new JsonMapper();
	
	@Override
	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		
		if (changeBatch.getId() == 26585496153L) {
			System.out.println(changeBatch);
		}

		//System.out.println(Thread.currentThread().getName() + ": Processing changebatch " + changeBatch.getId());
		long changeTime = changeBatch.getCreateTime().getTime();
		for (EntityChange change : changeBatch.getEntityChanges()) {
			EntitySpec2 entitySpec = EntitySpec2.get(change.getEntityClass().getName());
			if (entitySpec == null) {
				//System.out.println("Ignored unconfigured change handler " + change);
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
			
			synchronized (changesetWorking) {
				b3entity.applyChange(changesetWorking[0], change, changeTime, masterMap, pushingMapper);
				changesetWorking[0].record(changeBatch.getId(), changeBatch.getCreateTime());
			}
		}
	}
}