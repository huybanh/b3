package com.betbrain.b3.data;

import java.util.Date;
import java.util.HashMap;

public class ChangeSet implements DBTrait {
	
	private final HashMap<String, ChangeSetItem> changeItems = new HashMap<>();
	
	private long lastBatchId;
	
	private Date lastBatchTime;
	
	private int changeCount;

	public void record(long id, Date createTime) {
		this.lastBatchId = id;
		this.lastBatchTime = createTime;
		changeCount++;
		
		if (changeCount % 1000 == 0) {
			System.out.println(Thread.currentThread().getName() + ": ChangeSet status: " +
					changeCount + " changes, consolidated size: " + changeItems.size());
		}
	}
	
	@Override
	public void put(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
		ChangeSetItem changeItem = changeItems.get(table.name + hashKey + rangeKey);
		if (changeItem == null) {
			changeItem = new ChangeSetItem(ChangeSetItem.PUT, table, hashKey, rangeKey, cells);
			changeItems.put(table.name + hashKey + rangeKey, changeItem);
		} else {
			changeItem.consolidatePut(cells);
		}
	}
	
	@Override
	public void update(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
		ChangeSetItem changeItem = changeItems.get(table.name + hashKey + rangeKey);
		if (changeItem == null) {
			changeItem = new ChangeSetItem(ChangeSetItem.UPDATE, table, hashKey, rangeKey, cells);
			changeItems.put(table.name + hashKey + rangeKey, changeItem);
		} else {
			changeItem.consolidateUpdate(cells);
		}
	}
	
	@Override
	public void delete(B3Table table, String hashKey, String rangeKey) {
		ChangeSetItem changeItem = changeItems.get(table.name + hashKey + rangeKey);
		if (changeItem == null) {
			changeItem = new ChangeSetItem(ChangeSetItem.DELETE, table, hashKey, rangeKey, null);
			changeItems.put(table.name + hashKey + rangeKey, changeItem);
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
	
	public void persist() {
		
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
		
		DynamoWorker.updateSetting(
				new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_ID, String.valueOf(lastBatchId)),
				new B3CellString(DynamoWorker.BUNDLE_CELL_LASTBATCH_DEPLOYED_ID, lastBatchTime.toString()),
				new B3CellString("CHANGESET_SIZE", String.valueOf(changeCount)));
	}
}

class ChangeSetItem {
	
	private final B3Table table;
	
	private final String hashKey;
	
	private final String rangeKey;
	
	//private B3Cell<?>[] cells;
	private final HashMap<String, B3Cell<?>> cells = new HashMap<>();
	
	private int changeAction;
	
	static final int PUT = 0;
	static final int UPDATE = 1;
	static final int DELETE = 2;

	ChangeSetItem(int action, B3Table table, String hashKey, String rangeKey, B3Cell<?>[] newCells) {
		super();
		this.changeAction = action;
		this.table = table;
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
		
		addCells(newCells);
	}
	
	private void addCells(B3Cell<?>[] newCells) {
		if (newCells != null) {
			for (B3Cell<?> one : newCells) {
				this.cells.put(one.columnName, one);
			}
		}
	}
	
	void consolidatePut(B3Cell<?>[] newCells) {
		this.changeAction = PUT;
		this.cells.clear();
		addCells(newCells);
	}
	
	void consolidateDelete() {
		this.changeAction = DELETE;
		this.cells.clear();;
	}
	
	void consolidateUpdate(B3Cell<?>[] updatedCells) {
		/*if (this.changeAction == PUT) {
			//this.changeAction = PUT;
			//merge cells
		} else if (this.changeAction == UPDATE) {
			//this.changeAction = UPDATE;
			//merge cells
		} else {
			//this.changeAction is DELETE
			throw new RuntimeException();
		}*/
		
		if (this.changeAction == DELETE) {
			throw new RuntimeException();
		}
		
		//change action is as same as previous
		//just replace existing cells with updated cells
		addCells(updatedCells);
	}
	
	void persist() {
		
		if (this.changeAction == DELETE) {
			DynamoWorker.delete(table, hashKey, rangeKey);
			
		} else {
			int index = 0;
			B3Cell<?>[] cellArray = new B3Cell<?>[cells.size()];
			for (B3Cell<?> one : cells.values()) {
				cellArray[index] = one;
				index++;
			}
			if (this.changeAction == PUT) {
				DynamoWorker.put(true, this.table, hashKey, rangeKey, cellArray);
			} else if (this.changeAction == UPDATE) {
				DynamoWorker.update(table, hashKey, rangeKey, cellArray);
			}
		}
	}
}
