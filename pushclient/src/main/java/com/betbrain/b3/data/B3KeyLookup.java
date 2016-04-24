package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3KeyLookup extends B3Key {
	
	private final String rangeKey;

	public B3KeyLookup(Entity entity, B3Table targetTable, int targetHash, String targetRange) {
		super();
		//EntitySpec<?, ?> spec = EntitySpecMapping.getSpec(entity.getClass().getName());
		//rangeKey = targetTable.shortName + spec.getPrefix() + entity.getId();
		this.rangeKey = ModelShortName.get(entity.getClass().getName()) + entity.getId() +
				B3Table.CELL_LOCATOR_SEP + targetTable.shortName + 
				targetHash + B3Table.CELL_LOCATOR_SEP + targetRange;
	}

	@Override
	boolean isDetermined() {
		return true;
	}

	@Override
	String getRangeKey() {
		return rangeKey;
	}

}
