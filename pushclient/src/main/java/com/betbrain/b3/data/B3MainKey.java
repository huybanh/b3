package com.betbrain.b3.data;

import java.util.ArrayList;

import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3MainKey<E extends Entity> extends B3Key {
	
	abstract EntitySpec2 getEntitySpec();
	
	public ArrayList<?> listEntities(final boolean revisions, final JsonMapper jsonMapper ) {
		return listEntities(revisions, getEntitySpec().b3class, jsonMapper);
	}

}
