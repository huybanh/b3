package com.betbrain.b3.data;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

@Deprecated
public class InitialDumpDeployer2 {
    
    private final JsonMapper jsonMapper = new JsonMapper();

	private final HashMap<String, HashMap<Long, Entity>> masterMap;
	
	public InitialDumpDeployer2(HashMap<String, HashMap<Long, Entity>> masterMap) {
		this.masterMap = masterMap;
	}
	
	public void initialPutMaster(ChangeSet db) {
		initialPutAllEntities(db);
		initialPutAllToMainTable(db, EntitySpec2.Event);
		initialPutAllToMainTable(db, EntitySpec2.BettingOffer);
		initialPutAllToMainTable(db, EntitySpec2.Outcome);
		initialPutAllToMainTable(db, EntitySpec2.EventInfo);
	}
	
	public void initialPutAllEntities(final DBTrait db) {

		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			final EntitySpec2 spec = EntitySpec2.get(entry.getKey());
			if (spec == null) {
				continue;
			}
			for (Entity entity : entry.getValue().values()) {
				B3KeyEntity entityKey = new B3KeyEntity(entity);
				db.put(B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(), 
						new B3CellString(B3Table.CELL_LOCATOR_THIZ, jsonMapper.serialize(entity)));
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private <E extends Entity> void initialPutAllToMainTable(final DBTrait db, final EntitySpec2 spec) {

		final HashMap<Long, Entity> allEntities = masterMap.get(spec.entityClass.getName());
		if (allEntities == null) {
			return;
		}
		final B3Table table = spec.mainTable;
		for (Entity entity : allEntities.values()) {
			B3Entity<E> b3entity = (B3Entity<E>) spec.newB3Entity();
			b3entity.entity = (E) entity;
			b3entity.buildDownlinks(false, masterMap, null);
			B3Key b3key = b3entity.createMainKey();
			
			//put linked entities to table main, lookup, link
			LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
			putToLookupAndLinkRecursively(db, table, b3key, b3Cells, null, b3entity, masterMap, jsonMapper);
			
			//put main entity to main table
			//B3Update update = new B3Update(table, b3key, b3Cells.toArray(new B3CellString[b3Cells.size()]));
			db.put(table, b3key.getHashKey(), b3key.getRangeKey(), 
					b3Cells.toArray(new B3CellString[b3Cells.size()]));
			
			//put main entity revision to main table
			if (b3entity.getSpec().revisioned) {
				b3key.setRevisionId("0");
				db.put(table, b3key.getHashKey(), b3key.getRangeKey(), 
						b3Cells.toArray(new B3CellString[b3Cells.size()]));
			}
			//entity table
			/*B3KeyEntity entityKey = new B3KeyEntity(entity);
			update = new B3Update(B3Table.Entity, entityKey, 
					new B3CellString(B3Table.CELL_LOCATOR_THIZ, JsonMapper.SerializeF(entity)));
			update.execute();*/
			
			//if (processedCount % 100 == 0) {
				//logger.info(table.name + ": deployed " + processedCount + " of " + subTotalCount);
			//}
		}
	}
	
	private static boolean useLookupAndLink = false;
	
	/**
	 * @param putToFile
	 * @param mainTable
	 * @param mainKey: Null if no actual puts required
	 * @param mainCells
	 * @param cellName
	 * @param b3entity
	 * @param masterMap
	 * @param jsonMapper
	 */
	public static <E extends Entity>void putToLookupAndLinkRecursively(DBTrait db,
			B3Table mainTable, B3Key mainKey, LinkedList<B3Cell<?>> mainCells, 
			final String cellName, B3Entity<?> b3entity,
			HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper jsonMapper) {
		
		String thisCellName;
		if (cellName == null) {
			thisCellName = B3Table.CELL_LOCATOR_THIZ;
		} else {
			thisCellName = cellName;
		}
		
		//put event to main
		B3CellString jsonCell = new B3CellString(thisCellName, jsonMapper.serialize(b3entity.entity));
		mainCells.add(jsonCell);
		
		//put event to lookup
		if (useLookupAndLink && mainKey != null && cellName != null) {
			B3KeyLookup lookupKey = new B3KeyLookup(
					b3entity.entity, mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), thisCellName);
			db.put(B3Table.Lookup, lookupKey.getHashKey(), lookupKey.getRangeKey());
		}
		
		EntityLink[] linkedEntities = b3entity.getDownlinkedEntities();
		if (linkedEntities != null) {
			for (EntityLink link : linkedEntities) {
				
				//link: From main entity -> linked entities
				if (link.linkedEntity != null) {
					link.linkedEntity.buildDownlinks(false, masterMap, jsonMapper);
				}
				if (useLookupAndLink && mainKey != null) {
					B3KeyLink linkKey = new B3KeyLink(link.linkedEntityClazz, link.linkedEntityId, b3entity.entity, link.name); //reverse link direction
					db.put(B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
					
					//commented out, as we can always find a link without information from lookup table
					//also, put link to lookup: Main entity -> link location
					//lookupKey = new B3KeyLookup(b3entity.entity, B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
					//update = new B3Update(B3Table.Lookup, lookupKey);
					//DynamoWorker.put(bundleId, update);
				}
				
				if (link.linkedEntity != null) {
					String childCellName;
					if (cellName == null) {
						childCellName = link.name;
					} else {
						childCellName = cellName + B3Table.CELL_LOCATOR_SEP + link.name;
					}
					putToLookupAndLinkRecursively(
							db, mainTable, mainKey, mainCells, childCellName, link.linkedEntity, masterMap, jsonMapper);
				}
			}
		}
	}
}
