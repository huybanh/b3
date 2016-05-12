package com.betbrain.b3.data;

import java.util.Arrays;
import java.util.List;

import com.betbrain.b3.pushclient.JsonMapper;

public class PutBean {

	private String hash;
	
	private String range;
	
	private List<B3Cell<?>> cells;
	
	public PutBean() {
		//for serialization
	}

	public PutBean(String hash, String range, B3Cell<?>[] cells) {
		super();
		this.hash = hash;
		this.range = range;
		this.cells = Arrays.asList(cells);
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public List<B3Cell<?>> getB3Cells() {
		return cells;
	}

	public void setB3Cells(List<B3Cell<?>> cells) {
		this.cells = cells;
	}
	
	public static void main(String[] args) {
		B3Cell<?>[] testCells = new B3Cell<?>[] {new B3CellInt("x", 1),
			new B3CellString("y", "z")};
		PutBean p = new PutBean("a", "b", testCells);
		
		String s = new JsonMapper().serialize(p);
		System.out.println(s);
		p = (PutBean) new JsonMapper().deserialize(s);
		System.out.println(new JsonMapper().serialize(p));
		
		DynamoWorker.openLocalWriters();
		DynamoWorker.putFile(new JsonMapper(), B3Table.Entity, "x", "y", testCells);
		DynamoWorker.putAllFromLocal(2);
	}
}
