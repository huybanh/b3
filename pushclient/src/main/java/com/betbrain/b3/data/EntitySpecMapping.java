package com.betbrain.b3.data;

import java.util.HashMap;

import com.betbrain.b3.data.spec.EventSpec;
import com.betbrain.b3.data.spec.SportSpec;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public enum EntitySpecMapping {

	//Entity class prefix is exact-two-character 
	Event("EV", new EventSpec()),
	Sport("SP", new SportSpec());
	
	public final EntitySpec<? extends Entity, ?> spec;
	
	private EntitySpecMapping(String prefix, EntitySpec<? extends Entity, ?> spec) {
		this.spec = spec;
		this.spec.setPrefix(prefix);
	}
	
	private static HashMap<String, EntitySpecMapping> allMetas;
	
	public static void initialize() {
		allMetas = new HashMap<String, EntitySpecMapping>();
		for (EntitySpecMapping em : EntitySpecMapping.values()) {
			allMetas.put(em.spec.targetClassName, em);
		}
	}
	
	public static EntitySpec<? extends Entity, ?> getSpec(String entityClassName) {
		EntitySpecMapping mapping = allMetas.get(entityClassName);
		if (mapping == null) {
			return null;
		}
		return mapping.spec;
	}
}
