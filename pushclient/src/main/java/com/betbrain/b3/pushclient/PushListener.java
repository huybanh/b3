package com.betbrain.b3.pushclient;

import java.util.List;

import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;

public class PushListener implements SEPCConnectorListener {

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		System.out.println("notifyEntityUpdates: " + changeBatch);
		for (EntityChange ec: changeBatch.getEntityChanges()) {
			System.out.println("Changed: " + ec);
		}
	}

	public void notifyInitialDump(List<? extends Entity> entityList) {
		System.out.println("notifyInitialDump: " + entityList.size());
	}

}
