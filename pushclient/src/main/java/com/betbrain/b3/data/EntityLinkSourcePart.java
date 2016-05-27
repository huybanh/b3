package com.betbrain.b3.data;

public class EntityLinkSourcePart {

	public final Class<?> entityClass;
	
	public final long entityId;
	
	public EntityLinkSourcePart(Class<?> entityClass, long entityId) {
		super();
		this.entityClass = entityClass;
		this.entityId = entityId;
	}
	
}
