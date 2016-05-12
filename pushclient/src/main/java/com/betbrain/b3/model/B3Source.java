package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.Source;

public class B3Source extends B3EntityLeaf<Source> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.Source;
	}

}
