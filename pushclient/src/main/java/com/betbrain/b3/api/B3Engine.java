package com.betbrain.b3.api;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.model.B3Location;
import com.betbrain.b3.model.B3Sport;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.Location;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class B3Engine { 
	
	public static void main(String[] args) {
		B3Engine b3 = new B3Engine();
		b3.listSports();
		b3.listCountries();
		b3.searchLeagues(1L, 77L);
	}

	public B3Engine() {
		DynamoWorker.initBundleCurrent();
	}
	
	public Sport[] listSports() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEntity entityKey = new B3KeyEntity(Sport.class);
		ArrayList<?> allSports = entityKey.listEntities(false, B3Sport.class, jsonMapper);
		Sport[] result = new Sport[allSports.size()];
		int index = 0;
		for (Object one : allSports) {
			result[index] = ((B3Sport) one).entity;
			System.out.println(result[index]);
			index++;
		}
		return result;
	}
	
	public Location[] listCountries() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEntity entityKey = new B3KeyEntity(Location.class);
		ArrayList<?> allLocs = entityKey.listEntities(false, B3Location.class, jsonMapper);
		Location[] result = new Location[allLocs.size()];
		int index = 0;
		for (Object one : allLocs) {
			Location loc = ((B3Location) one).entity;
			if (loc.getTypeId() == IDs.LOCATIONTYPE_COUNTRY) {
				result[index] = loc;
				System.out.println(result[index]);
			}
			index++;
		}
		return result;
	}
	
	/*public Location[] getCountries() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyLink linkKey = new B3KeyLink(B3LocationType.class, IDs.LOCATIONTYPE_COUNTRY, 
				B3Location.class, Location.PROPERTY_NAME_typeId);
		ArrayList<?> allLocs = linkKey.listLinks();
		Location[] result = new Location[allLocs.size()];
		int index = 0;
		for (Object one : allLocs) {
			Location loc = ((B3Location) one).entity;
			if (loc.getTypeId() == IDs.LOCATIONTYPE_COUNTRY) {
				result[index] = loc;
				System.out.println(result[index]);
			}
			index++;
		}
		return result;
	}*/
	
	public Event[] searchLeagues(Long sportId, Long countryId) {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEvent eventKey = new B3KeyEvent(null, IDs.EVENTTYPE_GENERICTOURNAMENT, (String) null);
		ArrayList<?> allLeagues = eventKey.listEntities(false, jsonMapper);
		Event[] result = new Event[allLeagues.size()];
		int index = 0;
		for (Object one : allLeagues) {
			Event e = ((B3Event) one).entity;
			if ((sportId == null || e.getSportId() == sportId)
					&& (countryId == null || e.getVenueId() == countryId)) {
				result[index] = e;
				System.out.println(result[index]);
			}
			index++;
		}
		return result;
	}
}
