package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3KeyLookup extends B3Key {
	
	private final String hashKey;
	private final String rangeKey;

	public B3KeyLookup(Entity entity, B3Table targetTable, String targetHash, String targetRange) {
		super();
		//EntitySpec<?, ?> spec = EntitySpecMapping.getSpec(entity.getClass().getName());
		//rangeKey = targetTable.shortName + spec.getPrefix() + entity.getId();
		this.hashKey = ModelShortName.get(entity.getClass().getName()) + entity.getId() +
				B3Table.CELL_LOCATOR_SEP + targetTable.shortName;
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
