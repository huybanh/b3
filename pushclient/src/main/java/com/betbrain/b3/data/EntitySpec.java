package com.betbrain.b3.data;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public abstract class EntitySpec<E extends Entity> {
	
	private static final String COLUMNNAME_ENTITY_SUFFIX = "_B3";
	
	protected final B3Table targetTable;
	
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

	protected abstract B3Key getB3KeyMain(E entity);

	//protected abstract B3Key getB3KeyLookup(E entity);

	//protected abstract B3Key getB3KeyRelation(E entity);
	
	protected abstract long getId(E e);
	
	/*HashMap<String, Long> getIDMap(E e) {
		HashMap<String, Long> map = new HashMap<String, Long>();
		getAllIDs(e, map);
		return map;
	}*/
	
	protected abstract void getAllIDs(E e, HashMap<String, Long> map);
	
	protected LinkedList<B3Cell<?>> getCellList(E entity) {

		HashMap<String, Long> idMap = new HashMap<String, Long>();
		getAllIDs(entity, idMap);
		LinkedList<B3Cell<?>> allCells = new LinkedList<B3Cell<?>>();
		for (Entry<String, Long> entry : idMap.entrySet()) {
			Long v = entry.getValue();
			if (v == null) {
				continue;
			}
			allCells.add(new B3CellLong(this.prefix + entry.getKey(), v));
		}
		String json = JsonMapper.SerializeF(entity);
		allCells.add(new B3CellString(this.prefix + COLUMNNAME_ENTITY_SUFFIX, json));
		return allCells;
	}
	
}
