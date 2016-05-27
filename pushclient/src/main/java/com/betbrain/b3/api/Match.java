package com.betbrain.b3.api;

import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventParticipantRelation;
import com.betbrain.sepc.connector.sportsmodel.Participant;

public class Match {

	public Event event;
	
	public LinkedList<Participant> participants = new LinkedList<>();
	
	public HashMap<Long, EventParticipantRelation> relations = new HashMap<>();
}
