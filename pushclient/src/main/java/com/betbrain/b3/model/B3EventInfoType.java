package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.EventInfoType;

public class B3EventInfoType extends B3EntityLeaf<EventInfoType> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.EventInfoType;
	}

}
