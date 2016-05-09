package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.OutcomeStatus;

public class B3OutcomeStatus extends B3EntityLeaf<OutcomeStatus> {
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.OutcomeStatus;
	}

}
