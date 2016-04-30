package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.B3Bundle;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Provider;

public class B3Provider extends B3Entity<Provider> {

	@Override
	public void getDownlinkedEntitiesInternal() {
		
	}

	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap,
			B3Bundle bundle, JsonMapper mapper) {
		
	}

}
