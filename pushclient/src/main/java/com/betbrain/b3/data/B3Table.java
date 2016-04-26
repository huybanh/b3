package com.betbrain.b3.data;

public enum B3Table {
	
	InitialDump("initial_name", null),
	ChangeBatch("change_batch", null),
	Lookup("lookup", null),
	Entity("entity", null),
	Link("entity_links", null),
	Event("event", "E"),
	EventInfo("event_info", "I"),
	BettingOffer("betting_offer", "B");
	
	public final String name;
	
	//exact one character
	public final String shortName;
	
	static final int DIST_FACTOR = 100;
	
	static final String CELL_LOCATOR_THIZ = "THIZ";
	static final String CELL_LOCATOR_SEP = "_";
	
	static final String KEY_SEP = "/";
	
	//name of lookup column, which contains hash key of target table
	static final String LOOKUP_CELL_TARGET_HASH = "H";
	
	//name of lookup column, which contains range key of target table
	static final String LOOKUP_CELL_TARGET_RANGE = "R";
	
	static final String EVENTKEY_MARKER_EVENT = "E";
	static final String EVENTKEY_MARKER_EVENTPART = "P";
	
	private B3Table(String tableName, String shortName) {
		this.name = tableName;
		this.shortName = shortName;
	}
}
