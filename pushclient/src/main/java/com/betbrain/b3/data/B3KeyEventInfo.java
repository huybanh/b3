package com.betbrain.b3.data;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEventInfo extends B3KeyEntitySupport {

	final Long sportId;
	
	final Long eventTypeId;
	
	//final Boolean eventPartFlag;
	
	final Long eventId;
	
	final Long eventInfoTypeId;
	
	final Long eventInfoId;

	public B3KeyEventInfo(Long sportId, Long eventTypeId, Long eventId, Long eventInfoTypeId, Long eventInfoId) {
		super();
		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		//this.eventPartFlag = eventPart;
		this.eventId = eventId;
		this.eventInfoTypeId = eventInfoTypeId;
		this.eventInfoId = eventInfoId;
	}

	public B3KeyEventInfo(Long sportId, Long eventTypeId, Long eventId) {
		super();
		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		//this.eventPartFlag = eventPart;
		this.eventId = eventId;
		this.eventInfoTypeId = null;
		this.eventInfoId = null;
	}
	
	@Override
	boolean isDetermined() {
		return sportId != null && eventTypeId != null && eventId != null &&
				eventInfoTypeId != null && eventInfoId != null;
	} 
	
	public String getHashKeyInternal() {
		if (sportId == null) {
			return null;
		}
		if (eventTypeId == null) {
			return sportId + B3Table.KEY_SEP;
		}
		/*if (eventPartFlag == null) {
			return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP;
		}
		String eventPartMarker = eventPartFlag ? 
				B3Table.EVENTKEY_MARKER_EVENTPART : B3Table.EVENTKEY_MARKER_EVENT;

		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker +
				Math.abs(eventId.hashCode() % 100);*/
		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventId;
	}
	
	@Override
	String getRangeKeyInternal() {
		
		return eventInfoTypeId + B3Table.KEY_SEP + eventInfoId;
		//return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker + eventId; 
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public <E extends Entity> ArrayList<E> listEntities(JsonMapper jsonMapper) {
		ArrayList<E> list = new ArrayList<E>();
		int i = hardLimit;
		ItemCollection<QueryOutcome> coll = DynamoWorker.query(B3Table.EventInfo, getHashKey());
		IteratorSupport<Item, QueryOutcome> it = coll.iterator();
		while (it.hasNext()) {
			if (--i <= 0) {
				break;
			}
			Item item = it.next();
			String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
			Entity entity = jsonMapper.deserializeEntity(json);
			System.out.println(entity);
			list.add((E) entity);
		}
		return list;
	}
}
