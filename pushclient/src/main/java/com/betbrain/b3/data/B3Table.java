package com.betbrain.b3.data;

public enum B3Table {
	
	InitialDump("initial_name"),
	ChangeBatch("change_batch"),
	Lookup("lookup"),
	Relation("Relation"),
	Event("event"),
	BettingOffer("betting_offer");
	
	public final String tableName;
	
	private B3Table(String tableName) {
		this.tableName = tableName;
	}
}
