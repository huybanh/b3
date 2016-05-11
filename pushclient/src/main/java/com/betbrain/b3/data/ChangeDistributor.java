package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

@Deprecated
public class ChangeDistributor {
	
	//private final HashMap<String, HashMap<Long, Entity>> cachedEntities;
	
	//keyed by entity short name + entity id
	private final HashMap<String, ChangeBase> undeployedChangeMap = new HashMap<>();
	//ArrayList<ChangeBase> undeployedChanges = new ArrayList<>();
	
	private ChangeQueue[] changeQueues;
	
	private int rotatingQueueIndex;

	private int deployCount;
	
	public ChangeDistributor(int queueCount, HashMap<String, HashMap<Long, Entity>> cachedEntityMap) {
		//this.cachedEntities = cachedEntityMap;
		changeQueues = new ChangeQueue[queueCount];
		for (int i = 0; i < queueCount; i++) {
			changeQueues[i] = new ChangeQueue(i, undeployedChangeMap);
		}
		
		for (int i = 0; i < queueCount; i++) {
			final int queueId = i;
			new Thread() {
				
				//JsonMapper mapper = new JsonMapper();
				
				public void run() {
					while (true) {
						ChangeBase change = changeQueues[queueId].dequeue();
						/*if (change == null) {
							try {
								Thread.sleep(1);
							} catch (InterruptedException e) {
								
							}
							continue;
						}*/
						//change.b3entity.postApplyChange(change, cachedEntities, mapper);
						DynamoWorker.delete(B3Table.SEPC, change.hashKey, change.rangeKey);
						undeployedChangeMap.remove("change.takeMapKey()");
					}
				}
			}.start();
		}
	}
	
	public void distribute(ChangeBase change, JsonMapper mapper) {
		
		System.out.println(Thread.currentThread().getName() + ": Processing change " + change.rangeKey);
		/*if (Source.class.getName().equals(change.getEntityClassName())) {
			if (change instanceof ChangeUpdateWrapper) {
				List<String> changedNames = ((ChangeUpdateWrapper) change).getB3PropertyNames();
				if (changedNames == null || 
						(changedNames.size() == 1 && changedNames.contains(Source.PROPERTY_NAME_lastCollectedTime))) {
					
					//skip this trivial change
					return;
				}
			}
		}*/
		
		EntitySpec2 entitySpec = EntitySpec2.get(change.getEntityClassName());
		if (entitySpec == null /*|| entitySpec.b3class == null*/) {
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
		
		//b3entity.setSpec(entitySpec);
		//change.entitySpec = b3entity.getSpec();
		//change.b3entity = b3entity;
		
		//if (b3entity.preApplyChange(change, cachedEntities)) {
			undeployedChangeMap.put("change.takeMapKey()", change);
			EntityLink[] downlinkedEntities = b3entity.getDownlinkedEntities();
			System.out.println("Downlink count: " + downlinkedEntities.length);
			if (downlinkedEntities != null) {
				for (EntityLink link : downlinkedEntities) {
					if (link.linkedEntity == null) {
						continue;
					}
					String entityShortName = link.linkedEntity.getSpec().shortName;
					long linkId = link.linkedEntity.entity.getId();
					ChangeBase precedentChange = undeployedChangeMap.get(entityShortName + linkId);
					if (precedentChange != null) {
						change.addPrecedent(precedentChange);
					}
				}
			}
			if (change.precedents != null) {
				System.out.println("Precedent update count: " + change.precedents.size());
			}
			Integer queueIndex = change.getPreferedQueueIndex();
			if (queueIndex != null) {
				System.out.println("Change got prefered queue: " + queueIndex);
			}
			if (queueIndex == null) {
				queueIndex = this.rotatingQueueIndex;
				this.rotatingQueueIndex++;
				if (rotatingQueueIndex == this.changeQueues.length) {
					rotatingQueueIndex = 0;
				}
			}
			changeQueues[queueIndex].enqueue(change);
			//b3entity.postApplyChange(change, cachedEntities, mapper);
		//}
		//DynamoWorker.delete(B3Table.SEPC, change.hashKey, change.rangeKey);
		
		if (deployCount == 0) {
			Date d = new Date(Long.parseLong(change.changeTime));
			DynamoWorker.updateSetting(
					new B3CellString(DynamoWorker.BUNDLE_CELL_DEPLOYSTATUS, DynamoWorker.BUNDLE_PUSHSTATUS_ONGOING),
					new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_ID, change.rangeKey),
					new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_TIMESTAMP, d.toString()));
		}
		deployCount++;
		if (deployCount == 1000) {
			deployCount = 0;
		}
	}
}

@Deprecated
class ChangeQueue {
	
	final int queueId;
	
	private final HashMap<String, ChangeBase> undeployedChangeMap;

	private final ArrayList<ChangeBase> changes = new ArrayList<>();
	
	ChangeQueue(int id, HashMap<String, ChangeBase> undeployedChangeMap) {
		this.queueId = id;
		this.undeployedChangeMap = undeployedChangeMap;
	}
	
	void enqueue(ChangeBase change) {
		synchronized (changes) {
			changes.add(change);
			changes.notifyAll();
			System.out.println("Queue " + this.queueId + ": " + changes.size());
		}
	}
	
	ChangeBase dequeue() {
		ChangeBase change;
		synchronized (changes) {
			while (true) {
				if (!changes.isEmpty()) {
					break;
				}

				System.out.println(Thread.currentThread().getName() + ": no changes found, wait...");
				try {
					changes.wait();
				} catch (InterruptedException e) {
					
				}
			}
			change = changes.remove(0);
		}
		
		if (change.precedents == null) {
			return change;
		}

		while (true) {
			ChangeBase oneUndeployedPrecedent = null;
			boolean wait = false;
			for (ChangeBase one : change.precedents) {
				if (undeployedChangeMap.containsKey("one.takeMapKey()")) {
					wait = true;
					oneUndeployedPrecedent = one;
					break;
				}
			}
			if (!wait) {
				System.out.println("Change has " + change.precedents.size() + " precedents, but all has been deployed.");
				break;
			}
			try {
				System.out.println(Thread.currentThread().getName() + 
						" sleeps now to wait for precedent updates: " + 
						"oneUndeployedPrecedent.takeMapKey()" + " in queue " + oneUndeployedPrecedent.queueId);
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				
			}
		}
		return change;
	}
}
