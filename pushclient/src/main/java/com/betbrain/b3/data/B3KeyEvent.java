package com.betbrain.b3.data;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEvent extends B3Key {
	
	private final String MARKER_EVENT = "E";
	private final String MARKER_EVENTPART = "P";

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
	
	@Override
	Integer getHashKey() {
		return 0; //TODO modulo of eventId?
	}
	
	@Override
	String getRangeKey() {
		
		if (sportId == null) {
			return null;
		}
		if (eventTypeId == null) {
			return sportId + SEP;
		}
		if (eventPartFlag == null) {
			return sportId + SEP + eventTypeId + SEP;
		}
		String eventPartMarker = eventPartFlag ? MARKER_EVENTPART : MARKER_EVENT;
		if (eventId == null) {
			return sportId + SEP + eventTypeId + SEP + eventPartMarker + SEP;
		}
		return sportId + SEP + eventTypeId + SEP + eventPartMarker + eventId; 
	}
}
