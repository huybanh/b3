package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.B3Bundle;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3EntityLeaf<E extends Entity> extends B3Entity<E> {

	@Override
	protected void getDownlinkedEntitiesInternal() {
		//no downlinks
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap,
			B3Bundle bundle, JsonMapper mapper) {
		//no downlinks
	}

}
