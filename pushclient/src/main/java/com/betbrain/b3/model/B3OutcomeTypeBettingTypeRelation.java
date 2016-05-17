package com.betbrain.b3.model;

import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.sepc.connector.sportsmodel.OutcomeTypeBettingTypeRelation;

public class B3OutcomeTypeBettingTypeRelation extends B3EntityLeaf<OutcomeTypeBettingTypeRelation> {

	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.OutcomeTypeBettingTypeRelation;
	}

}
