package com.betbrain.b3.api;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;

import com.betbrain.sepc.connector.sportsmodel.BettingType;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.Location;
import com.betbrain.sepc.connector.sportsmodel.Sport;

/**
 * 
 * 
 * @author huybanh
 *
 */
public interface B3Api {

	Sport[] searchSports();

	Location[] searchCountries(Long sportId);

	Event[] searchLeagues(Long sportId, Long countryId);

	Event[] searchMatches(long leagueId, Date fromTime, Date toTime);

	BettingType[] searchBettingTypes(Long matchId);
	
	EventPart[] searchEventParts(long matchId, Long bettingTypeId);
	
	HashSet<OutcomeParameter>[] searchParameters(Long matchId, Long bettingTypeId, Long eventPartId);

	LinkedList<DetailedOddsTableTrait> reportDetailedOddsTable(long matchId, long eventPartId, long bettingTypeId,
			Float paramFloat1, Float paramFloat2, Float paramFloat3, Boolean paramBoolean1, String paramString1);

}