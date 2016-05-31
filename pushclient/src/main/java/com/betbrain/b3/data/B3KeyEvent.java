package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.Event;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEvent extends B3MainKey<Event> {

	private final Long sportId;
	
	private final Long parentId;
	
	private final Long eventTypeId;
	
	//private final String startTime;
	
	private final Long eventId;
	
	public B3KeyEvent(Event event) {
		sportId = event.getSportId();
		eventTypeId = event.getTypeId();
		parentId = event.getParentId();
		//startTime = dateFormat.format(event.getStartTime());
		eventId = event.getId();
	}

	/*@Deprecated
	public B3KeyEvent(Long sportId, Long eventTypeId, Long eventId) {
		super();
		this.sportId = sportId;
		this.eventTypeId = eventTypeId;
		parentId = null;
		//this.startTime = null;
		this.eventId = eventId;
	}*/

	/*public B3KeyEvent(Long parentId, Long eventTypeId, Long eventId, Date startTime) {
		super();
		sportId = null;
		this.eventTypeId = eventTypeId;
		this.parentId = parentId;
		this.startTime = dateFormat.format(startTime);
		this.eventId = eventId;
	}*/

	public B3KeyEvent(Long parentId, Long typeId/*, String startTime*/) {
		super();
		sportId = null;
		this.parentId = parentId;
		this.eventTypeId = typeId;
		//this.startTime = startTime;
		this.eventId = null;
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
			if (eventId == null) {
				return null;
			}
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

			//parentId can be null
			/*if (parentId == null) {
				return null;
			}*/
			
			long pid;
			if (parentId == null) {
				pid = -1;
			} else {
				pid = parentId;
			}
			if (eventTypeId == null) {
				return pid + B3Table.KEY_SEP;
			}
			/*if (startTime == null) {
				return pid + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP;
			}*/
			if (eventId == null) {
				return pid + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP; 
			}
			return pid + B3Table.KEY_SEP + eventTypeId + B3Table.KEY_SEP + eventId;
		}
		
		return String.valueOf(eventId);
	}
}
