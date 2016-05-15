package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.EventInfo;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEventInfo extends B3MainKey<EventInfo> {
	
	private final Long eventId;
	
	private final Long eventPartId;
	
	private final Long eventInfoTypeId;
	
	private final Long eventInfoId;

	@Deprecated
	public B3KeyEventInfo(Long eventId, Long eventInfoTypeId, Long eventInfoId) {
		super();
		this.eventId = eventId;
		this.eventPartId = null;
		this.eventInfoTypeId = eventInfoTypeId;
		this.eventInfoId = eventInfoId;
	}

	public B3KeyEventInfo(Long eventId, Long eventPartId, Long eventInfoTypeId, Long eventInfoId) {
		super();
		this.eventId = eventId;
		this.eventPartId = eventPartId;
		this.eventInfoTypeId = eventInfoTypeId;
		this.eventInfoId = eventInfoId;
	}

	public B3KeyEventInfo(Long eventId) {
		super();
		this.eventId = eventId;
		this.eventPartId = null;
		this.eventInfoTypeId = null;
		this.eventInfoId = null;
	}
	
	@Override
	B3Table getTable() {
		return B3Table.EventInfo;
	}
	
	@Override
	EntitySpec2 getEntitySpec() {
		return EntitySpec2.EventInfo;
	}
	
	@Override
	boolean isDetermined() {
		return eventId != null && eventPartId != null &&
				eventInfoTypeId != null && eventInfoId != null;
	} 
	
	public String getHashKeyInternal() {
		if (version2) {
			return Math.abs(eventId % B3Table.DIST_FACTOR) + "";
		}
		return String.valueOf(eventId);
	}
	
	@Override
	String getRangeKeyInternal() {
		if (version2) {
			if (eventPartId == null) {
				return null;
			}
			if (eventInfoTypeId == null) {
				return eventPartId + B3Table.KEY_SEP;
			}
			if (eventInfoId == null) {
				return eventPartId + B3Table.KEY_SEP + eventInfoTypeId + B3Table.KEY_SEP;
			}
			return eventPartId + B3Table.KEY_SEP + eventInfoTypeId + B3Table.KEY_SEP + eventInfoId;
		}
		if (eventInfoId == null) {
			return eventInfoTypeId + B3Table.KEY_SEP;
		}
		return eventInfoTypeId + B3Table.KEY_SEP + eventInfoId;
		//return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker + eventId; 
	}
	
	/*@Override
	@SuppressWarnings("unchecked")
	public <E extends Entity> ArrayList<E> listEntities(JsonMapper jsonMapper) {
		ArrayList<E> list = new ArrayList<E>();
		B3ItemIterator it = DynamoWorker.query(B3Table.EventInfo, getHashKey());
		while (it.hasNext()) {
			Item item = it.next();
			String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
			Entity entity = jsonMapper.deserializeEntity(json);
			System.out.println(entity);
			list.add((E) entity);
		}
		return list;
	}*/
}
