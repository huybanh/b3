package com.betbrain.b3.data;

import java.util.Date;

import com.betbrain.sepc.connector.sportsmodel.Event;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEvent extends B3MainKey<Event> {

	private final Long sportId;
	
	private final Long eventTypeId;
	
	private final String startTime;
	
	private final Long eventId;
	
	public B3KeyEvent(Event event) {
		startTime = dateFormat.format(event.getStartTime());
		eventId = event.getId();
		sportId = null;
		eventTypeId = null;
	}

	@Deprecated
	public B3KeyEvent(Long sportId, Long eventTypeId, Long eventId) {
		super();
		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		this.eventId = eventId;
		this.startTime = null;
	}

	public B3KeyEvent(Long eventId, Date startTime) {
		super();
		this.startTime = dateFormat.format(startTime);
		this.eventId = eventId;
		sportId = null;
		eventTypeId = null;
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
		return /*sportId != null && eventTypeId != null &&*/ eventId != null;
	} 
	
	protected String getHashKeyInternal() {
		if (version2) {
			return Math.abs(eventId % B3Table.DIST_FACTOR) + "";
		}
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
		if (version2) {
			return startTime + B3Table.KEY_SEP + eventId;
		}
		
		return String.valueOf(eventId);
	}
}
