package com.betbrain.b3.data;

import java.util.HashMap;

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

    /**
     * Required for serialization
     * 
     * @return
     */
    public Entity getEntity() {
    	if (create != null) {
    		return create.getEntity();
    	}
    	return entity;
    }

	@Override
    public Entity lookupEntity(HashMap<String, HashMap<Long, Entity>> masterMap) {
    	if (create != null) {
    		return create.getEntity();
    	}
    	return entity;
    }

	@Override
    public Long getEntityId() {
        
    	if (entity != null) {
    		return entity.getId();
    	}
        return null;
    }

	@Override
	public String getEntityClassName() {
		if (entity == null) {
			return null;
		}
		return entity.getClass().getName();
	}
	
	@Override
	public boolean isOnlyEntityMainIDsNeeded(EntitySpec2 entitySpec) {
		return false;
	}
	
}