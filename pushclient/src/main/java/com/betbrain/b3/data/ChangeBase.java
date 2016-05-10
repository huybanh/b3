package com.betbrain.b3.data;

import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.b3.model.B3Entity;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.util.beans.BeanUtil;

public abstract class ChangeBase {
	
	public String hashKey;
	
	public String rangeKey;
	
	public String changeTime;
	
	public int queueId;
	
	//public boolean deployed = false;
	
	LinkedList<ChangeBase> precedents;
	
	public EntitySpec2 entitySpec;
	
	public B3Entity<?> b3entity;

	public abstract String getEntityClassName();
	
	@Override
	public String toString() {
		return BeanUtil.toString(this);
	}
	
	public abstract Entity lookupEntity(HashMap<String, HashMap<Long, Entity>> masterMap);
	
	public abstract Long getEntityId();
	
	public abstract boolean isOnlyEntityMainIDsNeeded(EntitySpec2 entitySpec);

	void addPrecedent(ChangeBase precedentChange) {
		if (precedents == null) {
			precedents = new LinkedList<>();
		}
		precedents.add(precedentChange);
	}
	
	Integer getPreferedQueueIndex() {
		if (precedents == null) {
			return null;
		}
		return precedents.getFirst().queueId;
	}
}
