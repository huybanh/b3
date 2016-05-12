package com.betbrain.b3.data;

public interface DBTrait {

	public void put(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells);

	public void update(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells);
	
	public void delete(B3Table table, String hashKey, String rangeKey);
}
