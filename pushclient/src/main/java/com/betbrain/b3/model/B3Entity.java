package com.betbrain.b3.model;

import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.b3.data.EntityInitialPutHandler;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3Entity<E extends Entity/*, K extends B3Key*/> {

	public E entity;
	
	protected B3Entity() {
	}
	
	/*protected B3Entity(E entity) {
		this.entity = entity;
	}*/
	
	//abstract public K getB3KeyMain();
	
	private LinkedList<EntityLink> downlinks;
	
	abstract protected void getDownlinkedEntitiesInternal();
	
	protected final void addDownlink(String name, B3Entity<?> linkedEntity) {
		if (linkedEntity == null) {
			return;
		}
		downlinks.add(new EntityLink(name, linkedEntity));
	}
	
	public final EntityLink[] getDownlinkedEntities() {
		downlinks = new LinkedList<EntityLink>();
		getDownlinkedEntitiesInternal();
		EntityLink[] links = downlinks.toArray(new EntityLink[downlinks.size()]);
		downlinks = null;
		return links;
	}
	
	abstract public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap);
	
	@SuppressWarnings("rawtypes")
	static <E extends B3Entity, F> E build(Long id, E e, Class<? extends Entity> clazz,
			HashMap<String, HashMap<Long, Entity>> masterMap) {
		
		return build(id, e, clazz,masterMap, true);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static <E extends B3Entity> E build(Long id, E e, Class<? extends Entity> clazz,
			HashMap<String, HashMap<Long, Entity>> masterMap, boolean depthBuilding) {
		
		if (id == null) {
			return null;
		}
		HashMap<Long, Entity> subMap = masterMap.get(clazz.getName());
		if (subMap == null) {
			EntityInitialPutHandler.linkingErrors.add("Found zero entities of " + clazz.getName());
			return null;
		}
		Entity one = subMap.get(id);
		if (one == null) {
			EntityInitialPutHandler.linkingErrors.add("Missed ID " + id + " of " + clazz.getName());
			return null;
		}
		e.entity = one;
		if (depthBuilding) {
			e.buildDownlinks(masterMap);
		}
		return e;
	}

}
