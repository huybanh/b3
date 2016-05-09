package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.BettingType;

public class B3BettingType extends B3EntityLeaf<BettingType> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.BettingOfferType;
	}

}
