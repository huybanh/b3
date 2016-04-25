package com.betbrain.b3.data;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *           /outcomeTypeId/outcomeId/bettingTypeId/offerId
 *
 */
public class B3KeyOffer extends B3Key {

	final Long sportId;
	
	final Long eventTypeId;
	
	final Boolean eventPartFlag;
	
	final Long eventId;

	final Long outcomeTypeId;
	
	final Long outcomeId;
	
	final Long bettingTypeId;
	
	final Long offerId;

	public B3KeyOffer(Long sportId, Long eventTypeId, Boolean eventPart, Long eventId,
			Long outcomeTypeId, Long outcomeId, Long bettingTypeId, Long offerId) {

		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		this.eventPartFlag = eventPart;
		this.eventId = eventId;
		
		this.outcomeTypeId = outcomeTypeId;
		this.outcomeId = outcomeId;
		this.bettingTypeId = bettingTypeId;
		this.offerId = offerId;
	}
	
	@Override
	boolean isDetermined() {
		return sportId != null && eventTypeId != null && eventPartFlag != null & eventId != null &&
				outcomeTypeId != null && outcomeId != null && bettingTypeId != null && offerId != null;
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
		if (bettingTypeId == null) {
			return outcomeTypeId + B3Table.KEY_SEP + outcomeId;
		}
		if (offerId == null) {
			return outcomeTypeId + B3Table.KEY_SEP + outcomeId + 
					B3Table.KEY_SEP + bettingTypeId;
		}
		return outcomeTypeId + B3Table.KEY_SEP + outcomeId + 
				B3Table.KEY_SEP + bettingTypeId + B3Table.KEY_SEP + offerId; 
	}
}
