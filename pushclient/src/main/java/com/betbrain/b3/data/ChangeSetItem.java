package com.betbrain.b3.data;

import java.util.HashMap;

public class ChangeSetItem {
	
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
	
	public void persist() {
		
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

