package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.Participant;

public class B3Participant extends B3EntityLeaf<Participant> {

	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.Participant;
	}

}
