package com.betbrain.b3.data;

import java.util.HashMap;
import java.util.List;

import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Source;

public class ChangeDistributor {
	
	private static final HashMap<String, Entity> masterChangeMap = new HashMap<>();
	
	private static ChangeQueue[] changeQueues;
	
	public static void initChangeQueues(int queueCount) {
		changeQueues = new ChangeQueue[queueCount];
		for (int i = 0; i < queueCount; i++) {
			changeQueues[i] = new ChangeQueue(i);
		}
	}
	
	public static void distribute(ChangeBase change, 
			HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		
		/*if (true) {
			System.out.println(change);
			return;
		}*/
		if (Source.class.getName().equals(change.getEntityClassName())) {
			if (change instanceof ChangeUpdateWrapper) {
				List<String> changedNames = ((ChangeUpdateWrapper) change).getB3PropertyNames();
				if (changedNames == null || 
						(changedNames.size() == 1 && changedNames.contains(Source.PROPERTY_NAME_lastCollectedTime))) {
					
					//skip this trivial change
					return;
				}
			}
		}
		
		EntitySpec2 entitySpec = EntitySpec2.get(change.getEntityClassName());
		if (entitySpec == null /*|| entitySpec.b3class == null*/) {
			System.out.println("Ignoring unconfigured change handler " + change);
			return;
		}
		
		B3Entity<?> b3entity;
		try {
			b3entity = entitySpec.b3class.newInstance();
			b3entity.setSpec(entitySpec);
			b3entity.applyChange(change.changeTime, change, masterMap, mapper);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		DynamoWorker.delete(B3Table.SEPC, change.hashKey, change.rangeKey);
	}
}
