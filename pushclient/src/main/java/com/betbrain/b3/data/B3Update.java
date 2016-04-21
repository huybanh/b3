package com.betbrain.b3.data;

import java.util.LinkedList;

class B3Update {

	final B3Table table;
	
	final int hashKey;
	
	final String rangeKey;
	
	final B3Cell<?>[] cells;

	B3Update(B3Table table, B3Key key, LinkedList<B3Cell<?>> cellList) {
		super();
		this.table = table;
		this.hashKey = key.getHashKey();
		this.rangeKey = key.getRangeKey();
		this.cells = cellList.toArray(new B3Cell<?>[cellList.size()]);
	}

	@Override
	public String toString() {
		
		String head = "UPDATE " + table.tableName + ": (" + hashKey + ", " + rangeKey + ")";
		String tail = null;
		for (int i = 0; i < cells.length; i++) {
			String s = cells[i].columnName + ":" + cells[i].getTypeName() + " " + cells[i].value;
			if (tail == null) {
				tail = s;
			} else {
				tail = tail + ", " + s;
			}
		}
		if (tail == null) {
			return head;
		} else {
			return head + ", " + tail;
		}
	}

	public void execute() {
		System.out.println(this);
	}
}
