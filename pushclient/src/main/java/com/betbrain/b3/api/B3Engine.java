package com.betbrain.b3.api;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.model.B3BettingType;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.model.B3Location;
import com.betbrain.b3.model.B3OutcomeTypeBettingTypeRelation;
import com.betbrain.b3.model.B3Sport;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.IDs;
import com.betbrain.b3.report.detailedodds.DetailedOddsTable2;
import com.betbrain.b3.report.detailedodds.DetailedOddsTableData;
import com.betbrain.sepc.connector.sportsmodel.BettingType;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.Location;
import com.betbrain.sepc.connector.sportsmodel.OutcomeTypeBettingTypeRelation;
import com.betbrain.sepc.connector.sportsmodel.Sport;

/**
 * B3Engine provides high level API to access data, reports in AWS dynamodb.
 * 
 * Instantiation of B3Engine is heavy. Clients are recommended to instantiate B3Engine once,
 * and reuse the instance for all subsequent requests.
 * 
 * Thread safety: all methods of B3Engine are thread-safe
 * 
 * @author huybanh
 */
public class B3Engine implements B3Api {
	
	private final HashMap<Long, LinkedList<Long>> outcomeTypesByBettingType = new HashMap<>();
	
	public static void main(String[] args) {
		B3Api b3 = new B3Engine();
		//b3.listSports();
		//Location[] result = b3.listCountries();
		//b3.searchLeagues(1L, 77L);
		//System.out.println("Matches");
		//b3.searchMatches(215754838, null, null);
		//b3.listBettingTypes();
		LinkedList<DetailedOddsTableData> result = b3.reportDetailedOddsTable(219900664L, 3L, 47L, null, null, null, null, null);
		
		for (Object o : result) {
			System.out.println(o);
		}
	}

	public B3Engine() {
		DynamoWorker.initBundleCurrent();
		
		B3KeyEntity entityKey = new B3KeyEntity(OutcomeTypeBettingTypeRelation.class);
		JsonMapper jsonMapper = new JsonMapper();
		@SuppressWarnings("unchecked")
		ArrayList<B3OutcomeTypeBettingTypeRelation> relations = 
				(ArrayList<B3OutcomeTypeBettingTypeRelation>) entityKey.listEntities(false, B3OutcomeTypeBettingTypeRelation.class, jsonMapper);
		for (B3OutcomeTypeBettingTypeRelation one : relations) {
			System.out.println(one.entity.getId() + ": " + one.entity.getBettingTypeId() + "->" + one.entity.getOutcomeTypeId());
			LinkedList<Long> outcomeTypeIdList = outcomeTypesByBettingType.get(one.entity.getBettingTypeId());
			if (outcomeTypeIdList == null) {
				outcomeTypeIdList = new LinkedList<>();
				outcomeTypesByBettingType.put(one.entity.getBettingTypeId(), outcomeTypeIdList);
			}
			outcomeTypeIdList.add(one.entity.getOutcomeTypeId());
		}
	}
	
	public Long[] getOutcomeTypeIds(long bettingTypeId) {
		LinkedList<Long> idList = outcomeTypesByBettingType.get(bettingTypeId);
		if (idList == null) {
			return new Long[0];
		} else {
			return idList.toArray(new Long[idList.size()]);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#listSports()
	 */
	@Override
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
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#listCountries()
	 */
	@Override
	public Location[] listCountries() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEntity entityKey = new B3KeyEntity(Location.class);
		ArrayList<?> allLocs = entityKey.listEntities(false, B3Location.class, jsonMapper);
		LinkedList<Location> result = new LinkedList<>();
		Iterator<?> it = allLocs.iterator();
		while (it.hasNext()) {
			Location loc = ((B3Location) it.next()).entity;
			if (loc.getTypeId() == IDs.LOCATIONTYPE_COUNTRY) {
				result.add(loc);
			}
		}
		return result.toArray(new Location[result.size()]);
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
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#searchLeagues(java.lang.Long, java.lang.Long)
	 */
	@Override
	public Event[] searchLeagues(Long sportId, Long countryId) {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEvent eventKey = new B3KeyEvent(null, IDs.EVENTTYPE_GENERICTOURNAMENT, (String) null);
		ArrayList<?> allLeagues = eventKey.listEntities(false, jsonMapper);
		LinkedList<Event> result = new LinkedList<>();
		Iterator<?> it = allLeagues.iterator();
		while (it.hasNext()) {
			Event e = ((B3Event) it.next()).entity;
			if ((sportId == null || e.getSportId() == sportId)
					&& (countryId == null || e.getVenueId() == countryId)) {
				result.add(e);
			}
		}
		return result.toArray(new Event[result.size()]);
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#searchMatches(long, java.util.Date, java.util.Date)
	 */
	@Override
	public Event[] searchMatches(long leagueId, Date fromTime, Date toTime) {
		JsonMapper jsonMapper = new JsonMapper();
		String fromTimeString = null;
		if (fromTime != null) {
			fromTimeString = B3Key.dateFormat.format(fromTime);
		}
		String toTimeString = null;
		if (toTime != null) {
			toTimeString = B3Key.dateFormat.format(toTime);
		}
		B3KeyEvent eventKey = new B3KeyEvent(leagueId, IDs.EVENTTYPE_GENERICMATCH, fromTimeString);
		eventKey.rangeKeyEnd = toTimeString;
		
		@SuppressWarnings("unchecked")
		ArrayList<B3Event> matches = (ArrayList<B3Event>) eventKey.listEntities(false, jsonMapper);
		Event[] result = new Event[matches.size()];
		int index = 0;
		for (Object one : matches) {
			result[index] = ((B3Event) one).entity;
			System.out.println(result[index]);
			index++;
		}
		return result;
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#listBettingTypes()
	 */
	@Override
	public BettingType[] listBettingTypes() {
		JsonMapper jsonMapper = new JsonMapper();
		B3KeyEntity entityKey = new B3KeyEntity(BettingType.class);
		ArrayList<?> allSports = entityKey.listEntities(false, B3BettingType.class, jsonMapper);
		BettingType[] result = new BettingType[allSports.size()];
		int index = 0;
		for (Object one : allSports) {
			result[index] = ((B3BettingType) one).entity;
			System.out.println(result[index]);
			index++;
		}
		return result;
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.api.B3Api#reportDetailedOddsTable(long, long, long, java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Boolean, java.lang.String)
	 */
	@Override
	public LinkedList<DetailedOddsTableData> reportDetailedOddsTable(long matchId, long eventPartId, long bettingTypeId,
			Float paramFloat1, Float paramFloat2, Float paramFloat3, Boolean paramBoolean1, String paramString1) {
		DetailedOddsTable2 report = new DetailedOddsTable2(this, matchId, eventPartId, bettingTypeId,
				paramFloat1, paramFloat2, paramFloat3, paramBoolean1, paramString1);
		report.run();
		return report.outputData;
	}
}
