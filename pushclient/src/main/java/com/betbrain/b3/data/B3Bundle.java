package com.betbrain.b3.data;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

class B3Bundle {

	private final String id;
	
	Table offerTable;
	Table eventTable;
	Table eventInfoTable;
	Table outcomeTable;
	Table lookupTable;
	Table linkTable;
	Table entityTable;
	Table sepcTable;
	
	static B3Bundle workingBundle;
	
	private B3Bundle(String id) {
		this.id = id;
	}
	
	static void initWorkingBundle(DynamoDB dynamoDB, String id) {
		workingBundle = new B3Bundle(id);
		workingBundle.init(dynamoDB);
	}
	
	static String getWorkingBundleId() {
		return workingBundle.id;
	}
	
	private void init(DynamoDB dynamoDB) {

		offerTable = dynamoDB.getTable("offer");
		eventTable = dynamoDB.getTable("event");
		eventInfoTable = dynamoDB.getTable("event_info");
		outcomeTable = dynamoDB.getTable("outcome");
		lookupTable = dynamoDB.getTable("lookup");
		linkTable = dynamoDB.getTable("link");
		entityTable = dynamoDB.getTable("entity");
		sepcTable = dynamoDB.getTable("sepc");
	}
	
	Table getTable(B3Table b3table) {

		if (b3table == B3Table.BettingOffer) {
			return offerTable;
		} else if (b3table == B3Table.Event) {
			return eventTable;
		} else if (b3table == B3Table.EventInfo) {
			return eventInfoTable;
		} else if (b3table == B3Table.Outcome) {
			return outcomeTable;
		} else if (b3table == B3Table.Lookup) {
			return lookupTable;
		} else if (b3table == B3Table.Link) {
			return linkTable;
		} else if (b3table == B3Table.Entity) {
			return entityTable;
		} else if (b3table == B3Table.SEPC) {
			return sepcTable;
		} else {
			throw new RuntimeException("Unmapped table: " + b3table);
		}
	}
}
