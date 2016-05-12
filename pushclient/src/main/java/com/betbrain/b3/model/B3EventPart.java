package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.EventPart;

public class B3EventPart extends B3EntityLeaf<EventPart> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.EventPart;
	}

}
