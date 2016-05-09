package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.EventType;

public class B3EventType extends B3EntityLeaf<EventType> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.EventType;
	}

}
