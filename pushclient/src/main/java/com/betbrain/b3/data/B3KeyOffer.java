package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.BettingOffer;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *           /outcomeTypeId/outcomeId/bettingTypeId/offerId
 *
 */
public class B3KeyOffer extends B3MainKey<BettingOffer> {

	private final Long sportId;
	
	private final Long eventTypeId;
	
	private final Long eventId;
	
	private final Long eventPartId;

	private final Long outcomeTypeId;
	
	private final Long outcomeId;
	
	private final Long bettingTypeId;
	
	private final Long offerId;
	
	public B3KeyOffer(Long eventId, Long eventPartId,
			Long outcomeTypeId, Long outcomeId, Long bettingTypeId, Long offerId) {

		this.sportId = null;
		this.eventTypeId = null;
		this.eventId = eventId;
		this.eventPartId = eventPartId;
		
		this.outcomeTypeId = outcomeTypeId;
		this.outcomeId = outcomeId;
		this.bettingTypeId = bettingTypeId;
		this.offerId = offerId;
	}

	@Deprecated
	public B3KeyOffer(Long sportId, Long eventTypeId, Long eventId,
			Long outcomeTypeId, Long outcomeId, Long bettingTypeId, Long offerId) {

		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		this.eventId = eventId;
		this.eventPartId = null;
		
		this.outcomeTypeId = outcomeTypeId;
		this.outcomeId = outcomeId;
		this.bettingTypeId = bettingTypeId;
		this.offerId = offerId;
	}

	@Deprecated
	public B3KeyOffer(Long sportId, Long eventTypeId, Long eventId) {

		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		this.eventId = eventId;
		this.eventPartId = null;
		
		this.outcomeTypeId = null;
		this.outcomeId = null;
		this.bettingTypeId = null;
		this.offerId = null;
	}
	
	@Override
	B3Table getTable() {
		return B3Table.BettingOffer;
	}
	
	@Override
	EntitySpec2 getEntitySpec() {
		return EntitySpec2.BettingOffer;
	}
	
	@Override
	boolean isDetermined() {
		return /*sportId != null && eventTypeId != null &&*/ eventId != null && eventPartId != null &&
				outcomeTypeId != null && outcomeId != null && bettingTypeId != null && offerId != null;
	}
	
	@Override
	public String getHashKeyInternal() {
		if (version2) {
			return Math.abs(eventId % B3Table.DIST_FACTOR) + "";
		}
		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventId;
	}
	
	@Override
	String getRangeKeyInternal() {
		if (version2) {
			if (eventPartId == null) {
				return null;
			}
			if (outcomeTypeId == null) {
				return eventPartId + B3Table.KEY_SEP;
			}
			if (outcomeId == null) {
				return eventPartId + B3Table.KEY_SEP + outcomeTypeId;
			}
			if (bettingTypeId == null) {
				return eventPartId + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP + outcomeId;
			}
			if (offerId == null) {
				return eventPartId + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP + outcomeId + 
						B3Table.KEY_SEP + bettingTypeId;
			}
			return eventPartId + B3Table.KEY_SEP + outcomeTypeId + B3Table.KEY_SEP + outcomeId + 
					B3Table.KEY_SEP + bettingTypeId + B3Table.KEY_SEP + offerId;
		}
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
