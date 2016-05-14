package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Event;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEvent extends B3MainKey<Event> {

	final Long sportId;
	
	final Long eventTypeId;
	
	final Long eventId;

	public B3KeyEvent(Long sportId, Long eventTypeId, Long eventId) {
		super();
		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		this.eventId = eventId;
	}
	
	@Override
	B3Table getTable() {
		return B3Table.Event;
	}
	
	@Override
	EntitySpec2 getEntitySpec() {
		return EntitySpec2.Event;
	}
	
	@Override
	boolean isDetermined() {
		return sportId != null && eventTypeId != null && eventId != null;
	} 
	
	protected String getHashKeyInternal() {
		if (sportId == null) {
			return null;
		}
		if (eventTypeId == null) {
			return sportId + B3Table.KEY_SEP;
		}
		return sportId + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + 
				Math.abs(eventId.hashCode() % B3Table.DIST_FACTOR);
	}
	
	@Override
	String getRangeKeyInternal() {
		
		return String.valueOf(eventId);
	}
}
