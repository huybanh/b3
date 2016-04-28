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
public class B3KeyOutcome extends B3KeyEntitySupport {

	final Long sportId;
	
	final Long eventTypeId;
	
	final Boolean eventPartFlag;
	
	final Long eventId;

	final Long outcomeTypeId;
	
	final Long outcomeId;

	public B3KeyOutcome(Long sportId, Long eventTypeId, Boolean eventPart, Long eventId,
			Long outcomeTypeId, Long outcomeId) {

		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		this.eventPartFlag = eventPart;
		this.eventId = eventId;
		
		this.outcomeTypeId = outcomeTypeId;
		this.outcomeId = outcomeId;
	}
	
	@Override
	boolean isDetermined() {
		return sportId != null && eventTypeId != null && eventPartFlag != null & eventId != null &&
				outcomeTypeId != null && outcomeId != null;
	}
	
	@Override
	String getHashKey() {
		if (sportId == null) {
			return null;
		}
		if (eventTypeId == null) {
			return sportId + B3Table.KEY_SEP;
		}
		if (eventPartFlag == null) {
			return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP;
		}
		String eventPartMarker = eventPartFlag ? 
				B3Table.EVENTKEY_MARKER_EVENTPART : B3Table.EVENTKEY_MARKER_EVENT;
		if (eventId == null) {
			return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker;
		}
		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker + eventId;
	}
	
	@Override
	String getRangeKey() {
		if (outcomeTypeId == null) {
			return null;
		}
		if (outcomeId == null) {
			return String.valueOf(outcomeTypeId);
		}
		return outcomeTypeId + B3Table.KEY_SEP + outcomeId; 
	}
	
	public B3Outcome loadFull(B3Bundle bundle) {
		Item item = DynamoWorker.get(B3Table.Outcome, bundle, getHashKey(), getRangeKey());
		if (item == null) {
			System.out.println("ID not found: " + getHashKey() + "@" + getRangeKey());
			return null;
		}
		
		B3Outcome outcome = new B3Outcome();
		outcome.loadFull(item);
		return outcome;
	}
}
