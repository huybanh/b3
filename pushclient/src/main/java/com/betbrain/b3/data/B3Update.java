package com.betbrain.b3.data;

class B3Update {

	final B3Table table;
	
	final int hashKey;
	
	final String rangeKey;
	
	final B3Cell<?>[] cells;

	/*B3Update(B3Table table, B3Key key, B3Cell<?>... cell) {
		this(table, key, new B3Cell<?>[] {cell});
	}

	B3Update(boolean c, B3Table table, B3Key key, B3Cell<?>... cell) {
		this(table, key, new B3Cell<?>[] {cell});
	}

	B3Update(B3Table table, B3Key key, LinkedList<B3Cell<?>> cellList) {
		this(table, key, cellList == null ? null : cellList.toArray(new B3Cell<?>[cellList.size()]));
	}*/

	B3Update(B3Table table, B3Key key, B3Cell<?>... cells) {
		super();
		this.table = table;
		this.hashKey = key.getHashKey();
		this.rangeKey = key.getRangeKey();
		this.cells = cells;
	}

	@Override
	public String toString() {
		
		String head = "UPDATE " + table.name + ": (" + hashKey + ", " + rangeKey + ")";
		String tail = null;
		if (cells != null) {
			for (int i = 0; i < cells.length; i++) {
				String s = cells[i].columnName + ":" + cells[i].getTypeName() + " " + cells[i].value;
				if (tail == null) {
					tail = s;
				} else {
					tail = tail + ", " + s;
				}
			}
		}
		if (tail == null) {
			return head;
		} else {
			return head + ", " + tail;
		}
	}

	public void execute() {
		
		DynamoWorker.put(this);
		/*if (cells == null || cells.length == 0) {
			DynamoWorker.put(dynaTable, hashKey, rangeKey, null, null);
			System.out.println(this);
		} else {
			for (int i = 0; i < cells.length; i++) {
				//String s = cells[i].columnName + ":" + cells[i].getTypeName() + " " + cells[i].value;
				//if (cells[i] instanceof B3CellString) {
					DynamoWorker.put(dynaTable, hashKey, rangeKey, cells[i].columnName, ((B3CellString) cells[i]).value);
					System.out.println(this);
				//}
			}
		}*/
	}
}
