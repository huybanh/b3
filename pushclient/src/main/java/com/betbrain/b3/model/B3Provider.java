package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.Provider;

public class B3Provider extends B3EntityLeaf<Provider> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.Provider;
	}

}
