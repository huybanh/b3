package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.LocationType;

public class B3LocationType extends B3EntityLeaf<LocationType> {

	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.LocationType;
	}

}
