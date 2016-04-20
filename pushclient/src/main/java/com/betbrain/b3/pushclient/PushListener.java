package com.betbrain.b3.pushclient;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;

public class PushListener implements SEPCConnectorListener {
	
	private static boolean first = true;

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		for (EntityChange change: changeBatch.getEntityChanges()) {
			
			if (first) {
				System.out.println("First changed entity: " + change);
			}
			
			String entClass = change.getEntityClass().getName();
			/*if (entClass.contains(".Betting") || entClass.startsWith(".Event") ||
					entClass.startsWith(".Outcome")) {
				
				if (!entClass.contains("Participant")) {
					System.out.println("Changed: " + change);
				}
			}*/
			if (entClass.contains(".Source")) {
				//System.out.println("Changed source: " + change);
				if (change instanceof EntityUpdate) {
					long id = ((EntityUpdate) change).getEntityId();
					Integer count = sourceMap.get(id);
					if (count == null) {
						count = 0;
					}
					sourceMap.put(id, ++count);
				}
			}
			
			Integer i = map.get(entClass);
			if (i == null) {
				i = 0;
			}
			map.put(entClass, i + 1);
			
			if (System.currentTimeMillis() - lastConsole > 10000) {
				System.out.println("========= " + new Date() + 
						" / after (sec): " + (System.currentTimeMillis() - firstConsole)/1000);
				LinkedList<String> keys = new LinkedList<String>(map.keySet());
				Collections.sort(keys);
				for (String s : keys) {
					System.out.println(s + ":" + map.get(s));
				}
				
				System.out.println("Source update frequencies:");
				LinkedList<Long> sources = new LinkedList<Long>(sourceMap.keySet());
				Collections.sort(sources);
				int c = 0;
				for (Long id : sources) {
					System.out.println(id + ":" + sourceMap.get(id));
					if (c++ > 10) {
						break;
					}
				}
				lastConsole = System.currentTimeMillis();
			}
		}
		first = false;
	}
	
	private static long firstConsole = System.currentTimeMillis();
	private static long lastConsole;
	private static HashMap<String, Integer> map = new HashMap<String, Integer>();
	private static HashMap<Long, Integer> sourceMap = new HashMap<Long, Integer>();

	public void notifyInitialDump(List<? extends Entity> entityList) {
		System.out.println("notifyInitialDump: " + entityList.size());
		//run1: Read batch [1] containing 81 entities. [153 batches left]
		//run2: Read batch [1] containing 81 entities. [153 batches left]
		//next day
		//run3: Read batch [1] containing 81 entities. [159 batches left]
		//run3: notifyInitialDump: 2514123
		for (Entity e : entityList) {
			e.getName();
			e.getId();
		}
	}

}
