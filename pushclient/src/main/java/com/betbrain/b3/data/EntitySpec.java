package com.betbrain.b3.data;

import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.sepc.connector.sportsmodel.Entity;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public abstract class EntitySpec<E extends Entity, K extends B3Key> {
	
	//private static final String COLUMNNAME_ENTITY_SUFFIX = "_B3";
	
	protected final B3Table targetTable;
	
	//exact two characters
	private String prefix;

	protected final String targetClassName;
	
	protected EntitySpec(B3Table targetTable, /*String prefix,*/ String targetClassName) {
		this.targetTable = targetTable;
		//this.prefix = prefix;
		this.targetClassName = targetClassName;
	}
	
	protected String getPrefix() {
		return this.prefix;
	}
	
	protected void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	protected abstract K getB3KeyMain(E entity);
	
	protected K getB3KeyMainInitially(E entity, 
			HashMap<String, HashMap<Long, Entity>> masterMap,
			HashMap<Long, Long> eventPartToEventMap) {
		return null;
	}

	//protected abstract B3Key getB3KeyLookup(E entity);

	//protected abstract B3Key getB3KeyRelation(E entity);

	protected long getId(E e) {
		return e.getId();
	}
	
	/*HashMap<String, Long> getIDMap(E e) {
		HashMap<String, Long> map = new HashMap<String, Long>();
		getAllIDs(e, map);
		return map;
	}*/
	
	protected abstract void getAllDownlinks(E e, LinkedList<EntityLink> downLinks);
	
	/*protected LinkedList<B3Cell<?>> getCellList(E entity) {

		LinkedList<EntityLink> downLinks = new LinkedList<EntityLink>();
		getAllDownlinks(entity, downLinks);
		LinkedList<B3Cell<?>> allCells = new LinkedList<B3Cell<?>>();
		for (EntityLink link : downLinks) {
			if (link.targetId == null) {
				continue;
			}
			//allCells.add(new B3CellLong(this.prefix + entry.getKey(), v));
			//allCells.add(new B3CellString(link.name, JsonMapper.Serialize(entity)));
		}
		String json = JsonMapper.SerializeF(entity);
		allCells.add(new B3CellString(this.prefix + COLUMNNAME_ENTITY_SUFFIX, json));
		return allCells;
	}*/
	
}
