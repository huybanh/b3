package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class B3Sport extends B3EntityLeaf<Sport> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.Sport;
	}

}
