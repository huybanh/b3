package com.betbrain.b3.pushclient;

import java.util.List;

import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.EntityCreate;
import com.betbrain.sepc.connector.sportsmodel.EntityDelete;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;

public class PushListener implements SEPCConnectorListener {
	
	private static boolean first = false;

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		//long lastBatchId = changeBatch.getId();
		if (first) {
			return;
		}
		System.out.println("notifyEntityUpdates: " + changeBatch);
		//System.out.println("notifyEntityUpdates: batch id: " + changeBatch.getId());
		//first = true;
		for (EntityChange change: changeBatch.getEntityChanges()) {
			System.out.println("Changed: " + change);
			//ec could be: EntityCreate/EntityUpdate/EntityDelete
			if (change instanceof EntityCreate) {
				Entity e = ((EntityCreate) change).getEntity();
				//e.getId();
				//e.getName();
				System.out.println("Changed entity: " + e);
			} else if (change instanceof EntityUpdate) {
				//Entity e = ((EntityUpdate) change).getEntityId();
			} else if (change instanceof EntityDelete) {
				long id = ((EntityDelete) change).getEntityId();
				//System.out.println("Changed entity: " + e);
			}
		}
	}

	public void notifyInitialDump(List<? extends Entity> entityList) {
		System.out.println("notifyInitialDump: " + entityList.size());
		//run1: Read batch [1] containing 81 entities. [153 batches left]
		//run2: Read batch [1] containing 81 entities. [153 batches left]
		for (Entity e : entityList) {
			e.getName();
			e.getId();
		}
	}

}
