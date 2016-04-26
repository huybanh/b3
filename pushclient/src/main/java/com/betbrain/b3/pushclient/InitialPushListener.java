package com.betbrain.b3.pushclient;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntityInitialPutHandler;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class InitialPushListener implements SEPCConnectorListener {
	
	public static void main(String[] args) {
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(new InitialPushListener());
		//pushConnector.setEntityChangeBatchProcessingMonitor(new BatchMonitor());
		pushConnector.start("OddsHistory");
	}

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		
	}
	
	private HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();
	
	//eventPartId -> eventId
	private HashMap<Long, Long> eventPartToEventMap = new HashMap<Long, Long>();

	public void notifyInitialDump(List<? extends Entity> entityList) {
		for (Entity e : entityList) {
			HashMap<Long, Entity> subMap = masterMap.get(e.getClass().getName());
			if (subMap == null) {
				subMap = new HashMap<Long, Entity>();
				masterMap.put(e.getClass().getName(), subMap);
			}
			subMap.put(e.getId(), e);
			
			if (e instanceof Event) {
				Event event = (Event) e;
				eventPartToEventMap.put(event.getRootPartId(), event.getId());
			}
		}
		System.out.println("Got all entities in memory");
		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue().size());
		}
		
		ModelShortName.initialize();
		DynamoWorker.initialize();
		new Thread() {
			public void run() {
				new EntityInitialPutHandler(masterMap/*, eventPartToEventMap*/).initialPutMaster();
			}
		}.start();
	}

}
