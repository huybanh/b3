package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityCreate;

public class ChangeCreateWrapper extends ChangeBase {
	
	private EntityCreate create;
	
	private Entity entity;
	
	//for deserialization
	public ChangeCreateWrapper() {
		
	}
	
	public ChangeCreateWrapper(EntityCreate create) {
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