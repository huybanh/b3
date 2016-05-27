package com.betbrain.b3.data;

import com.betbrain.sepc.connector.sportsmodel.EventParticipantRelation;

public class B3KeyEventParticipantRelation extends B3MainKey<EventParticipantRelation> {

	private final Long eventId;
	
	private final Long participantRoleId;

	private final Long eventPartId;
	
	private final Long participantId;
	
	private final Long parentParticipantId;
	
	public B3KeyEventParticipantRelation(Long eventId, Long participantRoleId, Long eventPartId, Long participantId,
			Long parentParticipantId) {
		super();
		this.eventId = eventId;
		this.participantRoleId = participantRoleId;
		this.eventPartId = eventPartId;
		this.participantId = participantId;
		this.parentParticipantId = parentParticipantId;
	}

	@Override
	EntitySpec2 getEntitySpec() {
		return EntitySpec2.EventParticipantRelation;
	}

	@Override
	B3Table getTable() {
		return B3Table.EPRelation;
	}

	@Override
	boolean isDetermined() {
		return false;
	}

	@Override
	String getHashKeyInternal() {
		if (eventId == null) {
			return null;
		}
		return Math.abs(eventId % B3Table.DIST_FACTOR) + "";
	}

	@Override
	String getRangeKeyInternal() {
		if (eventId == null) {
			return null; 
		}
		if (participantRoleId == null) {
			return eventId + B3Table.KEY_SEP;
		}
		if (eventPartId == null) {
			return eventId + B3Table.KEY_SEP + participantRoleId + B3Table.KEY_SEP;
		}
		if (participantId == null) {
			return eventId + B3Table.KEY_SEP + participantRoleId + B3Table.KEY_SEP + eventPartId + B3Table.KEY_SEP;
		}
		if (parentParticipantId == null) {
			return eventId + B3Table.KEY_SEP + participantRoleId + B3Table.KEY_SEP + eventPartId + B3Table.KEY_SEP +
					 participantId + B3Table.KEY_SEP;
		}
		return eventId + B3Table.KEY_SEP + participantRoleId + B3Table.KEY_SEP + eventPartId + B3Table.KEY_SEP +
				 participantId + B3Table.KEY_SEP + parentParticipantId;
	}

}
