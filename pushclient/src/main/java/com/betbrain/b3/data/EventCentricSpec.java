package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class EventCentricSpec<E extends Entity> extends EntitySpec<E> {

	protected EventCentricSpec(String targetClassName) {
		super(B3Table.Event, targetClassName);
	}

}
