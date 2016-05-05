package com.betbrain.b3.data;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *           /outcomeTypeId/outcomeId/bettingTypeId/offerId
 *
 */
public class B3KeyOffer extends B3KeyEntitySupport {

	final Long sportId;
	
	final Long eventTypeId;
	
	//final Boolean eventPartFlag;
	
	final Long eventId;

	final Long outcomeTypeId;
	
	final Long outcomeId;
	
	final Long bettingTypeId;
	
	final Long offerId;

	public B3KeyOffer(Long sportId, Long eventTypeId, Long eventId,
			Long outcomeTypeId, Long outcomeId, Long bettingTypeId, Long offerId) {

		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		//this.eventPartFlag = eventPart;
		this.eventId = eventId;
		
		this.outcomeTypeId = outcomeTypeId;
		this.outcomeId = outcomeId;
		this.bettingTypeId = bettingTypeId;
		this.offerId = offerId;
	}

	public B3KeyOffer(Long sportId, Long eventTypeId, Long eventId) {

		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		//this.eventPartFlag = eventPart;
		this.eventId = eventId;
		
		this.outcomeTypeId = null;
		this.outcomeId = null;
		this.bettingTypeId = null;
		this.offerId = null;
	}
	
	@Override
	boolean isDetermined() {
		return sportId != null && eventTypeId != null && eventId != null &&
				outcomeTypeId != null && outcomeId != null && bettingTypeId != null && offerId != null;
	}
	
	@Override
	public String getHashKeyInternal() {
		/*if (sportId == null) {
			return null;
		}
		if (eventTypeId == null) {
			return sportId + B3Table.KEY_SEP;
		}
		if (eventId == null) {
			return sportId + B3Table.KEY_SEP + eventTypeId;
		}*/
		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventId;
	}
	
	@Override
	String getRangeKeyInternal() {
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
