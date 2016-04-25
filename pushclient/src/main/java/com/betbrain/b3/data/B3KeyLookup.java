package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3KeyLookup extends B3Key {
	
	private final String hashKey;
	private final String rangeKey;

	public B3KeyLookup(Entity entity, B3Table targetTable, String targetHash, String targetRange) {
		super();
		this.hashKey = ModelShortName.get(entity.getClass().getName()) + targetTable.shortName + entity.getId();
		this.rangeKey = targetHash + B3Table.CELL_LOCATOR_SEP + targetRange;
	}

	@Override
	boolean isDetermined() {
		return true;
	}
	
	@Override
	String getHashKey() {
		return hashKey;
	}

	@Override
	String getRangeKey() {
		return rangeKey;
	}

}
