package com.betbrain.b3.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.b3.pushclient.JsonMapper;

public class ChangeSet implements DBTrait {
	
	private HashMap<String, ChangeSetItem> changesBeingConsolidated = new HashMap<>();

	private LinkedList<ChangeSetItem> changesBeingPersisted;
	
	private Long lastBatchId;
	
	private Date lastBatchTime;
	
	private int changeCount;
	
	public long getLastBatchId() {
		return lastBatchId;
	}
	
	public Date getLastBatchTime() {
		return lastBatchTime;
	}
	
	public boolean isEmpty() {
		return lastBatchId == null;
	}

	public void record(long id, Date createTime) {
		this.lastBatchId = id;
		this.lastBatchTime = createTime;
		changeCount++;
		
		if (changeCount % 5000 == 0) {
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
	
	public boolean isClosed() {
		return changesBeingConsolidated == null;
	}
	
	public ChangeSetItem checkout() {
		if (changesBeingPersisted.isEmpty()) {
			//System.out.println(Thread.currentThread().getName() + ": ChangeSet persisted: " +
			//		changeCount + " changes");
			return null;
		}
		return changesBeingPersisted.removeFirst();
	}
	
	public int getChangeSize() {
		return this.changeCount;
	}
	
	/*public int countChangesBeingPersisted() {
		return changesBeingPersisted.size();
	}*/
	
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
	
	public void toFile(String fileName) {
		try {
			System.out.println(Thread.currentThread().getName() + ": Filing changeset: " + fileName);
			JsonMapper mapper = new JsonMapper();
			BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, false));
			for (ChangeSetItem c : changesBeingPersisted) {
				c.toFile(writer, mapper);
			}
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void fromFile(String fileName) {
		try {
			System.out.println(Thread.currentThread().getName() + ": changeset from file " + fileName);
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			while (true) {
				String line = reader.readLine();
				if (line == null) {
					reader.close();
					new File(fileName).delete();
					return;
				}
				ChangeSetItem c = new ChangeSetItem(line);
				changesBeingPersisted.add(c);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
