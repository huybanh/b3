package com.betbrain.b3.data;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *           /outcomeTypeId/outcomeId/bettingTypeId/offerId
 *
 */
public class B3KeyOffer extends B3KeyEvent {

	final Long outcomeTypeId;
	
	final Long outcomeId;
	
	final Long bettingTypeId;
	
	final Long offerId;

	public B3KeyOffer(Long sportId, Long eventTypeId, Boolean eventPart, Long eventId,
			Long outcomeTypeId, Long outcomeId, Long bettingTypeId, Long offerId) {
		
		super(sportId, eventId, eventPart, eventId);
		this.outcomeTypeId = outcomeTypeId;
		this.outcomeId = outcomeId;
		this.bettingTypeId = bettingTypeId;
		this.offerId = offerId;
	}
	
	@Override
	boolean isDetermined() {
		return super.isDetermined() && outcomeTypeId != null && outcomeId != null &&
				bettingTypeId != null && offerId != null;
	}
	
	@Override
	Integer getHashKey() {
		return 0; //TODO modulo of eventId?
	}
	
	@Override
	String getRangeKey() {
		
		String eventRange = super.getRangeKey();
		if (eventPartFlag == null) {
			return null;
		}
		
		if (outcomeTypeId == null) {
			return eventRange + B3Table.KEY_SEP;
		}
		if (outcomeId == null) {
			return eventRange + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP;
		}
		if (bettingTypeId == null) {
			return eventRange + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP +
					outcomeId + B3Table.KEY_SEP;
		}
		if (offerId == null) {
			return eventRange + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP +
					outcomeId + B3Table.KEY_SEP + bettingTypeId + B3Table.KEY_SEP;
		}
		return eventRange + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP +
				outcomeId + B3Table.KEY_SEP + bettingTypeId + B3Table.KEY_SEP + offerId; 
	}
}
