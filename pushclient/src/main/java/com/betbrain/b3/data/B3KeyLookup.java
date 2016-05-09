package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3KeyLookup extends B3Key {
	
	private final String hashKey;
	private final String rangeKey;

	public B3KeyLookup(Entity entity, B3Table targetTable, String targetHash, String targetRange, String targetCell) {
		super();
		this.hashKey = EntitySpec2.get(entity.getClass().getName()).shortName + entity.getId();
		this.rangeKey = targetTable.shortName + targetHash + B3Table.CELL_LOCATOR_SEP + targetRange + B3Table.CELL_LOCATOR_SEP + targetCell;
	}

	public B3KeyLookup(Class<?> entityClazz, long entityId) {
		super();
		this.hashKey = EntitySpec2.get(entityClazz.getName()).shortName + entityId;
		this.rangeKey = null;
	}

	@Override
	boolean isDetermined() {
		return true;
	}
	
	@Override
	String getHashKeyInternal() {
		return hashKey;
	}

	@Override
	String getRangeKeyInternal() {
		return rangeKey;
	}

}
