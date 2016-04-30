package com.betbrain.b3.pushclient;

import com.betbrain.b3.model.B3Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityDelete;

public class EntityDeleteWrapper extends EntityChangeBase {

	private EntityDelete delete;
	
	private String entityClassName;
	
	private long entityId;
	
	//for deserialization
	public EntityDeleteWrapper() {
		
	}
	
	public EntityDeleteWrapper(EntityDelete delete) {
		this.delete = delete;
	}
	
	public void setEntityClassName(String className) {
		this.entityClassName = className;
	}
	
	public String getEntityClassName() {
        
		if (delete != null) {
			return delete.getEntityClass().getName();
		}
        return entityClassName;
    }
	
	public void setEntityId(long id) {
		this.entityId = id;
	}

    public long getEntityId() {
        
    	if (delete != null) {
    		return delete.getEntityId();
    	}
        return entityId;
    }

	@Override
	B3Entity<?> createB3Entity() {
		
		return null;
	}
}
