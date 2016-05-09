package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.EntityDelete;

public class ChangeDeleteWrapper extends ChangeBase {

	private EntityDelete delete;
	
	private String entityClassName;
	
	private long entityId;
	
	//for deserialization
	public ChangeDeleteWrapper() {
		
	}
	
	public ChangeDeleteWrapper(EntityDelete delete) {
		this.delete = delete;
	}
	
	public void setEntityClassName(String className) {
		this.entityClassName = className;
	}
	
	@Override
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
}
