package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.EventStatus;

public class B3EventStatus extends B3EntityLeaf<EventStatus> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.EventStatus;
	}

}
