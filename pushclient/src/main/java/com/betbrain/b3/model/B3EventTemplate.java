package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.EventTemplate;

public class B3EventTemplate extends B3EntityLeaf<EventTemplate> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.EventTemplate;
	}

}
