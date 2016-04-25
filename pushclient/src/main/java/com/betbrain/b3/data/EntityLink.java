package com.betbrain.b3.data;

import com.betbrain.b3.model.B3Entity;

public class EntityLink {

	final String name;
	
	final Long targetId;
	
	final Class<?> targetClass;
	
	final B3Entity<?> linkedEntity;

	public EntityLink(String name, Long targetId, Class<?> targetClass) {
		super();
		this.name = name;
		this.targetId = targetId;
		this.targetClass = targetClass;
		linkedEntity = null;
	}
	
	public EntityLink(String name, B3Entity<?> linkedEntity) {
		if (linkedEntity == null) {
			throw new NullPointerException();
		}
		this.name = name;
		this.targetId = linkedEntity.entity.getId();
		this.targetClass = linkedEntity.entity.getClass();
		this.linkedEntity = linkedEntity;
	}
}
