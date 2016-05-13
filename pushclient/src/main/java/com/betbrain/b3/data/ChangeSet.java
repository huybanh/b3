package com.betbrain.b3.data;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

public class ChangeSet implements DBTrait {
	
	private HashMap<String, ChangeSetItem> changesBeingConsolidated = new HashMap<>();

	private LinkedList<ChangeSetItem> changesBeingPersisted;
	
	private long lastBatchId;
	
	private Date lastBatchTime;
	
	private int changeCount;
	
	public long getLastBatchId() {
		return lastBatchId;
	}
	
	public Date getLastBatchTime() {
		return lastBatchTime;
	}

	public void record(long id, Date createTime) {
		this.lastBatchId = id;
		this.lastBatchTime = createTime;
		changeCount++;
		
		if (changeCount % 1000 == 0) {
			System.out.println(Thread.currentThread().getName() + ": ChangeSet status: " +
					changeCount + " changes, consolidated size: " + changesBeingConsolidated.size());
		}
	}
	
	public void close() {
		changesBeingPersisted = new LinkedList<ChangeSetItem>(changesBeingConsolidated.values());
		changesBeingConsolidated = null;
		if (changeCount > 0) {
			System.out.println(Thread.currentThread().getName() +
					": ChangeSet closed: " + changeCount + " changes");
		}
	}
	
	public ChangeSetItem checkout() {
		if (changesBeingPersisted.isEmpty()) {
			//System.out.println(Thread.currentThread().getName() + ": ChangeSet persisted: " +
			//		changeCount + " changes");
			return null;
		}
		return changesBeingPersisted.removeFirst();
	}
	
	public int countChangesBeingPersisted() {
		return changesBeingPersisted.size();
	}
	
	@Override
	public void put(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
		ChangeSetItem changeItem = changesBeingConsolidated.get(table.name + hashKey + rangeKey);
		if (changeItem == null) {
			changeItem = new ChangeSetItem(ChangeSetItem.PUT, table, hashKey, rangeKey, cells);
			changesBeingConsolidated.put(table.name + hashKey + rangeKey, changeItem);
		} else {
			changeItem.consolidatePut(cells);
		}
	}
	
	@Override
	public void update(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
		ChangeSetItem changeItem = changesBeingConsolidated.get(table.name + hashKey + rangeKey);
		if (changeItem == null) {
			changeItem = new ChangeSetItem(ChangeSetItem.UPDATE, table, hashKey, rangeKey, cells);
			changesBeingConsolidated.put(table.name + hashKey + rangeKey, changeItem);
		} else {
			changeItem.consolidateUpdate(cells);
		}
	}
	
	@Override
	public void delete(B3Table table, String hashKey, String rangeKey) {
		ChangeSetItem changeItem = changesBeingConsolidated.get(table.name + hashKey + rangeKey);
		if (changeItem == null) {
			changeItem = new ChangeSetItem(ChangeSetItem.DELETE, table, hashKey, rangeKey, null);
			changesBeingConsolidated.put(table.name + hashKey + rangeKey, changeItem);
		} else {
			changeItem.consolidateDelete();
		}
	}
	
	public void deleteCurrentLookup(Class<?> entityClass, long entityId) {
		//TODO it is incorrect to delete async
		/*B3KeyLookup lookupKey = new B3KeyLookup(entityClass, entityId);
		B3ItemIterator it = DynamoWorker.query(B3Table.Lookup, lookupKey.getHashKey());
		while (it.hasNext()) {
			Item item = it.next();
			changeSet.delete(B3Table.Lookup, lookupKey.getHashKey(), item.getString(DynamoWorker.RANGE));
		}*/
	}
	
	/*public void persist() {
		
		if (changeItems.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
			return;
		}

		System.out.println("Persisting changeset of " + changeCount + " changes");
		for (ChangeSetItem one : changeItems.values()) {
			one.persist();
		}
		
	}*/
}
