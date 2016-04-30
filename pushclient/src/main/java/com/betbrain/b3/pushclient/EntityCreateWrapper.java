package com.betbrain.b3.pushclient;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityCreate;

public class EntityCreateWrapper extends EntityChangeBase {
	
	private EntityCreate create;
	
	private Entity entity;
	
	//for deserialization
	public EntityCreateWrapper() {
		
	}
	
	public EntityCreateWrapper(EntityCreate create) {
		this.create = create;
	}
	
	public void setEntity(Entity entity) {
		this.entity = entity;
	}

    public Entity getEntity() {
    	if (create != null) {
    		return create.getEntity();
    	}
    	return entity;
    }

	@Override
	public String getEntityClassName() {
		if (entity == null) {
			return null;
		}
		return entity.getClass().getName();
	}
	
}