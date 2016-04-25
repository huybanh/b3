package com.betbrain.b3.data;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEvent extends B3Key {

	final Long sportId;
	
	final Long eventTypeId;
	
	final Boolean eventPartFlag;
	
	final Long eventId;

	public B3KeyEvent(Long sportId, Long eventTypeId, Boolean eventPart, Long eventId) {
		super();
		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		this.eventPartFlag = eventPart;
		this.eventId = eventId;
	}
	
	@Override
	boolean isDetermined() {
		return sportId != null && eventTypeId != null && eventPartFlag != null & eventId != null;
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

		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker + B3Table.KEY_SEP;
	}
	
	@Override
	String getRangeKey() {
		
		return String.valueOf(eventId);
		//return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventPartMarker + eventId; 
	}
}
