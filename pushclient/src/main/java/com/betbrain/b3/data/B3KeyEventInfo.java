package com.betbrain.b3.data;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEventInfo extends B3Key {

	final Long sportId;
	
	final Long eventTypeId;
	
	final Boolean eventPartFlag;
	
	final Long eventId;
	
	final Long eventInfoTypeId;
	
	final Long eventInfoId;

	public B3KeyEventInfo(Long sportId, Long eventTypeId, Boolean eventPart, Long eventId, Long eventInfoTypeId, Long eventInfoId) {
		super();
		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		this.eventPartFlag = eventPart;
		this.eventId = eventId;
		this.eventInfoTypeId = eventInfoTypeId;
		this.eventInfoId = eventInfoId;
	}
	
	@Override
	boolean isDetermined() {
		return sportId != null && eventTypeId != null && eventPartFlag != null && eventId != null &&
				eventInfoTypeId != null && eventInfoId != null;
	} 
	
	protected String getHashKey() {
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

		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker +
				Math.abs(eventId.hashCode() % 100);
	}
	
	@Override
	String getRangeKey() {
		
		return eventId + B3Table.KEY_SEP + eventInfoTypeId + B3Table.KEY_SEP + eventInfoId;
		//return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker + eventId; 
	}
}
