package com.betbrain.b3.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.util.beans.BeanUtil;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ChangeSetItem {
	
	private B3Table table;
	
	private String hashKey;
	
	private String rangeKey;
	
	//private B3Cell<?>[] cells;
	private HashMap<String, B3Cell<?>> cells = new HashMap<>();
	
	private static Gson gson = new Gson();
	
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
	
	public ChangeSetItem(String line) {
		fromFile(line);
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
	
	void toFile(BufferedWriter writer, JsonMapper mapper) throws IOException {
		writer.write(mapper.serialize(this));
		writer.newLine();
	}
	
	void fromFile(String line) {
		JsonObject json = gson.fromJson(line, JsonObject.class);
		this.table = B3Table.valueOf(json.get("tableName").getAsString());
		this.changeAction = json.get("changeAction").getAsInt();
		this.hashKey = json.get("hash").getAsString();
		JsonElement ele = json.get("range");
		if (ele != null && !ele.isJsonNull()) {
			this.rangeKey = ele.getAsString();
		} else {
			this.rangeKey = null;
		}
		ele = json.get("b3Cells");
		cells = null;
		if (ele != null && !ele.isJsonNull()) {
			JsonArray arr = ele.getAsJsonArray();
			cells = new HashMap<>();
			for (int i = 0; i < arr.size(); i++) {
				JsonObject oneCell = arr.get(i).getAsJsonObject();
				String colName = oneCell.get("columnName").getAsString();
				String colValue = oneCell.get("value").getAsString();
				B3CellString c = new B3CellString(colName, colValue);
				cells.put(c.columnName, c);
			}
		}
	}

	public String getHash() {
		return hashKey;
	}

	public void setHash(String hash) {
		this.hashKey = hash;
	}

	public String getRange() {
		return rangeKey;
	}

	public void setRange(String range) {
		this.rangeKey = range;
	}

	public String getTableName() {
		return table.name();
	}

	public void setTableName(String tableName) {
		this.table = B3Table.valueOf(tableName);
	}

	public int getChangeAction() {
		return changeAction;
	}

	public void setChangeAction(int changeAction) {
		this.changeAction = changeAction;
	}

	public Collection<B3Cell<?>> getB3Cells() {
		return cells.values();
	}

	public void setB3Cells(Collection<B3Cell<?>> loadedCells) {
		this.cells = new HashMap<>();
		for (B3Cell<?> c : loadedCells) {
			this.cells.put(c.columnName, c);
		}
	}
	
	public static void main(String[] args) {
		ChangeSetItem c = new ChangeSetItem(1, B3Table.BettingOffer, "h", "r", null);
		String s = new JsonMapper().serialize(c);
		System.out.println(s);
		c = new ChangeSetItem(s);
		System.out.println(BeanUtil.toString(c));
		
		c = new ChangeSetItem(1, B3Table.BettingOffer, "h", "r", new B3Cell[] {new B3CellString("x", "y")});
		s = new JsonMapper().serialize(c);
		System.out.println(s);
		c = new ChangeSetItem(s);
		System.out.println(BeanUtil.toString(c));
	}
}

