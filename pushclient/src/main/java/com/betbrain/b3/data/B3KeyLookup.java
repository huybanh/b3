package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3KeyLookup extends B3Key {
	
	private final String rangeKey;

	public B3KeyLookup(Entity entity) {
		super();
		EntitySpec<?> spec = EntitySpecMapping.getSpec(entity.getClass().getName());
		rangeKey = spec.getPrefix() + SEP + entity.getId();
	}

	@Override
	boolean isDetermined() {
		return true;
	}

	@Override
	Integer getHashKey() {
		return 0; //TODO proper hashing
	}

	@Override
	String getRangeKey() {
		return rangeKey;
	}

}
