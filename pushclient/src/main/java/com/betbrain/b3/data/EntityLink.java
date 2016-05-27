package com.betbrain.b3.data;

import com.betbrain.b3.model.B3Entity;

public class EntityLink {

	public final String name;
	
	public final Long linkedEntityId;
	
	public final Class<?> linkedEntityClazz;
	
	final B3Entity<?> linkedEntity;
	
	public final EntityLinkSourcePart[] sourceParts;

	//for SPECs : no longer used
	/*public EntityLink(String name, Long targetId, Class<?> linkedEntityClazz) {
		super();
		this.name = name;
		this.linkedEntityId = targetId;
		this.linkedEntityClazz = linkedEntityClazz;
		linkedEntity = null;
	}*/
	
	public static EntityLink createCrossLink(Class<?> linkedEntityClazz, Long linkedEntityId, EntityLinkSourcePart... sourceParts) {
		return new EntityLink(null, linkedEntityClazz, linkedEntityId, sourceParts);
	}
	
	private EntityLink(String name, Class<?> linkedEntityClazz, Long linkedEntityId, EntityLinkSourcePart[] sourceParts) {
		super();
		this.name = name;
		this.linkedEntityClazz = linkedEntityClazz;
		this.linkedEntityId = linkedEntityId;
		linkedEntity = null;
		
		this.sourceParts = sourceParts;
	}
	
	public EntityLink(String name, Class<?> linkedEntityClazz, Long linkedEntityId) {
		super();
		this.name = name;
		this.linkedEntityClazz = linkedEntityClazz;
		this.linkedEntityId = linkedEntityId;
		linkedEntity = null;
		
		this.sourceParts = null;
	}
	
	public EntityLink(String name, B3Entity<?> linkedEntity) {
		/*if (linkedEntity == null) {
			throw new NullPointerException();
		}*/
		this.name = name;
		this.linkedEntity = linkedEntity;
		if (linkedEntity != null) {
			this.linkedEntityId = linkedEntity.entity.getId();
			this.linkedEntityClazz = linkedEntity.entity.getClass();
		} else {
			this.linkedEntityId = null;
			this.linkedEntityClazz = null;
		}

		this.sourceParts = null;
	}
	
	public String getLinkName() {
		return this.name;
	}
}
