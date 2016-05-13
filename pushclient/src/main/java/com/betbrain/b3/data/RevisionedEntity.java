package com.betbrain.b3.data;

public class RevisionedEntity<E> {

	public final long time;
	
	public final E b3entity;
	
	public RevisionedEntity(long time, E b3entity) {
		super();
		this.time = time;
		this.b3entity = b3entity;
	}

}
