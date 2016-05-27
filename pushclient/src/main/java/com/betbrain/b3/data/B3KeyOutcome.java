package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Outcome;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *           /outcomeTypeId/outcomeId/bettingTypeId/offerId
 *
 */
public class B3KeyOutcome extends B3MainKey<Outcome> {

	private final Long sportId;
	
	private final Long eventTypeId;
	
	private final Long eventId;
	
	private final Long eventPartId;

	private final Long outcomeTypeId;
	
	private final Long outcomeId;

	public B3KeyOutcome(Long eventId, Long eventPartId, Long outcomeTypeId, Long outcomeId) {

		this.sportId = null;
		this.eventTypeId = null;
		this.eventId = eventId;
		this.eventPartId = eventPartId;
		
		this.outcomeTypeId = outcomeTypeId;
		this.outcomeId = outcomeId;
	}

	@Deprecated
	public B3KeyOutcome(Long sportId, Long eventTypeId, Long eventId, Long eventPartId,
			Long outcomeTypeId, Long outcomeId) {

		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
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
		return /*sportId != null && eventTypeId != null &&*/ eventPartId != null & eventId != null &&
				outcomeTypeId != null && outcomeId != null;
	}
	
	@Override
	String getHashKeyInternal() {
		if (version2) {
			return Math.abs(eventId % B3Table.DIST_FACTOR) + "";
		}
		if (sportId == null) {
			return null;
		}
		if (eventTypeId == null) {
			return sportId + B3Table.KEY_SEP;
		}
		if (eventId == null) {
			return sportId + B3Table.KEY_SEP + eventTypeId;
		}
		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventId;
	}
	
	@Override
	String getRangeKeyInternal() {
		if (version2) {
			if (eventId == null) {
				return null;
			}
			if (eventPartId == null) {
				return eventId + B3Table.KEY_SEP;
			}
			if (outcomeTypeId == null) {
				return eventId + B3Table.KEY_SEP + eventPartId + B3Table.KEY_SEP;
			}
			if (outcomeId == null) {
				return eventId + B3Table.KEY_SEP + eventPartId + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP;
			}
			return eventId + B3Table.KEY_SEP + eventPartId + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP + outcomeId;
		}
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
	
	/*public B3Outcome loadFull(JsonMapper mapper) {
		Item item = DynamoWorker.get(B3Table.Outcome, getHashKey(), getRangeKey());
		if (item == null) {
			System.out.println("ID not found: " + getHashKey() + "@" + getRangeKey());
			return null;
		}
		
		B3Outcome outcome = new B3Outcome();
		outcome.loadFull(item, mapper);
		return outcome;
	}*/
}
