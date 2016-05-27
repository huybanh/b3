package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.B3KeyEventParticipantRelation;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EventParticipantRelation;

public class B3EventParticipantRelation extends B3Entity<EventParticipantRelation> {

	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.EventParticipantRelation;
	}

	@Override
	void getDownlinkedEntitiesInternal() {
		
	}

	@Override
	public void buildDownlinks(boolean forMainKeyOnly, HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		
	}
	
	@Override
	String canCreateMainKey() {
		if (entity == null) {
			return "Null entity";
		}
		return null;
	}

	@Override
	public B3KeyEventParticipantRelation createMainKey() {
		if (entity == null) {
			return null;
		}
		return new B3KeyEventParticipantRelation(
				entity.getEventId(), entity.getParticipantRoleId(), 
				entity.getEventPartId(), entity.getParticipantId(), entity.getParentParticipantId());
		
	}

}
