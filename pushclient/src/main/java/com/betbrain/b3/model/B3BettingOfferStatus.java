package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus;

public class B3BettingOfferStatus extends B3EntityLeaf<BettingOfferStatus> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.BettingOfferStatus;
	}

}
