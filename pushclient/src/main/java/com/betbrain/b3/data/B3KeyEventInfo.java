package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.EventInfo;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEventInfo extends B3MainKey<EventInfo> {

	//private final Long sportId;
	
	//private final Long eventTypeId;
	
	//final Boolean eventPartFlag;
	
	private final Long eventId;
	
	private final Long eventInfoTypeId;
	
	private final Long eventInfoId;

	public B3KeyEventInfo(/*Long sportId, Long eventTypeId,*/ Long eventId, Long eventInfoTypeId, Long eventInfoId) {
		super();
		//this.sportId = sportId;
		//this.eventTypeId = eventTypeId;
		//this.eventPartFlag = eventPart;
		this.eventId = eventId;
		this.eventInfoTypeId = eventInfoTypeId;
		this.eventInfoId = eventInfoId;
	}

	public B3KeyEventInfo(/*Long sportId, Long eventTypeId,*/ Long eventId) {
		super();
		//this.sportId = sportId;
		//this.eventTypeId = eventTypeId;
		//this.eventPartFlag = eventPart;
		this.eventId = eventId;
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
		return /*sportId != null && eventTypeId != null &&*/ eventId != null &&
				eventInfoTypeId != null && eventInfoId != null;
	} 
	
	public String getHashKeyInternal() {
		/*if (sportId == null) {
			return null;
		}
		if (eventTypeId == null) {
			return sportId + B3Table.KEY_SEP;
		}
		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventId;*/
		return String.valueOf(eventId);
	}
	
	@Override
	String getRangeKeyInternal() {
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
