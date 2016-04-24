package com.betbrain.b3.pushclient;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;

public class PushListener implements SEPCConnectorListener {

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		
	}
	
	private HashMap<String, LinkedList<Entity>> map = new HashMap<String, LinkedList<Entity>>();

	public void notifyInitialDump(List<? extends Entity> entityList) {
		for (Entity e : entityList) {
			LinkedList<Entity> list = map.get(e.getClass().getName());
			if (list == null) {
				list = new LinkedList<Entity>();
				map.put(e.getClass().getName(), list);
			}
			list.add(e);
		}
		System.out.println("Got all entities in memory");
		
		//EntitySpecMapping.initialize();
		//new EntityInitialPutHandler().initialPut(map);
	}

}
