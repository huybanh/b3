package com.betbrain.b3.data;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.model.B3Outcome;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Outcome;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *           /outcomeTypeId/outcomeId/bettingTypeId/offerId
 *
 */
public class B3KeyOutcome extends B3MainKey<Outcome> {

	final Long sportId;
	
	final Long eventTypeId;
	
	//final Boolean eventPartFlag;
	
	final Long eventId;
	
	final Long eventPartId;

	final Long outcomeTypeId;
	
	final Long outcomeId;

	public B3KeyOutcome(Long sportId, Long eventTypeId, Long eventId, Long eventPartId,
			Long outcomeTypeId, Long outcomeId) {

		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		//this.eventPartFlag = eventPart;
		this.eventId = eventId;
		this.eventPartId = eventPartId;
		
		this.outcomeTypeId = outcomeTypeId;
		this.outcomeId = outcomeId;
	}
	
	@Override
	B3Table getTable() {
		return B3Table.Outcome;
	}
	
	@Override
	EntitySpec2 getEntitySpec() {
		return EntitySpec2.Outcome;
	}
	
	@Override
	boolean isDetermined() {
		return sportId != null && eventTypeId != null && eventPartId != null & eventId != null &&
				outcomeTypeId != null && outcomeId != null;
	}
	
	@Override
	String getHashKeyInternal() {
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
				B3Table.EVENTKEY_MARKER_EVENTPART : B3Table.EVENTKEY_MARKER_EVENT;*/
		if (eventId == null) {
			return sportId + B3Table.KEY_SEP + eventTypeId/* + B3Table.KEY_SEP + eventPartMarker*/;
		}
		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventId;
	}
	
	@Override
	String getRangeKeyInternal() {
		if (eventPartId == null) {
			return null;
		}
		if (outcomeTypeId == null) {
			return String.valueOf(eventPartId);
		}
		if (outcomeId == null) {
			return eventPartId + B3Table.KEY_SEP + outcomeTypeId;
		}
		return eventPartId + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP + outcomeId; 
	}
	
	public B3Outcome loadFull(JsonMapper mapper) {
		Item item = DynamoWorker.get(B3Table.Outcome, getHashKey(), getRangeKey());
		if (item == null) {
			System.out.println("ID not found: " + getHashKey() + "@" + getRangeKey());
			return null;
		}
		
		B3Outcome outcome = new B3Outcome();
		outcome.loadFull(item, mapper);
		return outcome;
	}
}
