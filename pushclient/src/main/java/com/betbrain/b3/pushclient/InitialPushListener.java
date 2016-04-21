package com.betbrain.b3.pushclient;

import java.util.HashMap;
import java.util.List;

import com.betbrain.b3.data.EntityInitialPutHandler;
import com.betbrain.b3.data.EntitySpecMapping;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;

public class InitialPushListener implements SEPCConnectorListener {
	
	public static void main(String[] args) {
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(new InitialPushListener());
		pushConnector.setEntityChangeBatchProcessingMonitor(new BatchMonitor());
		pushConnector.start("OddsHistory");
	}

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		
	}
	
	private HashMap<String, HashMap<Long, Entity>> map = new HashMap<String, HashMap<Long,Entity>>();

	public void notifyInitialDump(List<? extends Entity> entityList) {
		for (Entity e : entityList) {
			HashMap<Long, Entity> subMap = map.get(e.getClass().getName());
			if (subMap == null) {
				subMap = new HashMap<Long, Entity>();
				map.put(e.getClass().getName(), subMap);
			}
			subMap.put(e.getId(), e);
		}
		System.out.println("Got all entities in memory");
		
		EntitySpecMapping.initialize();
		new Thread() {
			public void run() {
				new EntityInitialPutHandler().initialPut(map);
			}
		}.start();
	}

}
