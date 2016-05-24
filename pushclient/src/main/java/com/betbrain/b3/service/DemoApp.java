package com.betbrain.b3.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.betbrain.b3.api.B3Engine;
import com.betbrain.b3.api.OutcomeParameter;
import com.betbrain.b3.report.detailedodds.DetailedOddsTable2;
import com.betbrain.sepc.connector.sportsmodel.BettingType;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.Location;
import com.betbrain.sepc.connector.sportsmodel.Sport;

@Path("/demoapp")
public class DemoApp {
	
	static B3Engine b3 = new B3Engine();
	
	@GET
	@Path("/sports")
	@Produces({MediaType.TEXT_HTML})
	public Response searchSports() {
		Sport[] sports = b3.searchSports();
		//return Response.status(200).entity("OK").build();
		return buildResponse(sports, ItemSport.class, "");
	}
	
	private static Response buildResponse(Object[] objs, Class<? extends Item<?>> clazz, String otherParams) {
		StringBuilder sb = new StringBuilder();
		for (Object o : objs) {
			Item<?> item;
			try {
				item = clazz.newInstance();
				item.setEntity(o);
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			sb.append(item.getHtml(otherParams));
		}
		return Response.status(200).entity(sb.toString()).build();
	}
	
	private static String buildQueryString(HashMap<String, String> params) {
		
		String s = "";
		for (Entry<String, String> e : params.entrySet()) {
			s += "&" + e.getKey() + "=" + e.getValue();
		}
		return s;
	}
	
	@GET
	@Path("/set")
	@Produces({MediaType.TEXT_HTML})
	public Response choose(
			@QueryParam("sportId") String sportId,
			@QueryParam("countryId") String countryId,
			@QueryParam("leagueId") String leagueId,
			@QueryParam("matchId") String matchId,
			@QueryParam("bettingTypeId") String bettingTypeId,
			@QueryParam("eventPartId") String eventPartId,
			@QueryParam("p1") String p1,
			@QueryParam("p2") String p2,
			@QueryParam("p3") String p3,
			@QueryParam("p4") String p4,
			@QueryParam("p5") String p5,
			
			@QueryParam("name") String name, 
			@QueryParam("value") String value) {
		
		HashMap<String, String> params = new HashMap<>();
		params.put("sportId", sportId);
		params.put("countryId", countryId);
		params.put("leagueId", leagueId);
		params.put("matchId", matchId);
		params.put("bettingTypeId", bettingTypeId);
		params.put("eventPartId", eventPartId);
		params.put("p1", p1);
		params.put("p2", p2);
		params.put("p3", p3);
		params.put("p4", p4);
		params.put("p5", p5);
		
		if (name.equals("sportId")) {
			params.put("sportId", value);
			Location[] locs = b3.searchCountries(Long.parseLong(value));
			return buildResponse(locs, ItemCountry.class, buildQueryString(params));
		} else if (name.equals("countryId")) {
			params.put("countryId", value);
			Event[] leagues = b3.searchLeagues(Long.parseLong(params.get("sportId")), Long.parseLong(value));
			return buildResponse(leagues, ItemLeague.class, buildQueryString(params));
		} else if (name.equals("leagueId")) {
			params.put("leagueId", value);
			Event[] matches = b3.searchMatches(Long.parseLong(value), null, null);
			return buildResponse(matches, ItemMatch.class, buildQueryString(params));
		} else if (name.equals("matchId")) {
			params.put("matchId", value);
			BettingType[] bettingTypes = b3.searchBettingTypes(Long.parseLong(value));
			return buildResponse(bettingTypes, ItemBettingType.class, buildQueryString(params));
		} else if (name.equals("bettingTypeId")) {
			params.put("bettingTypeId", value);
			EventPart[] eventParts = b3.searchEventParts(Long.parseLong(params.get("matchId")), Long.parseLong(value));
			return buildResponse(eventParts, ItemEventPart.class, buildQueryString(params));
		} else if (name.equals("eventPartId")) {
			params.put("eventPartId", value);
			OutcomeParameter[][] outcomeParams = b3.searchParameters(Long.parseLong(params.get("matchId")),
					Long.parseLong(params.get("bettingTypeId")), 
					Long.parseLong(value));
			if (outcomeParams != null && outcomeParams.length > 0 ) {
				return buildResponse(outcomeParams, ItemOutcomeParams.class, buildQueryString(params));
			} else {
				DetailedOddsTable2 report = new DetailedOddsTable2(
						b3, Long.parseLong(matchId), Long.parseLong(value), Long.parseLong(bettingTypeId), null);
				report.setPlainText(true);
				report.run();
				return Response.status(200).entity(new String(report.outStream.toByteArray())).build();
			}
		} else if (name.equals("outcomeParams")) {
			params.put("outcomeParams", value);
			HashSet<OutcomeParameter> outcomeParams = new HashSet<>();
			String[] paramArray = value.split("\\|");
			for (String oneParam : paramArray) {
				String[] nameValue = oneParam.split("\\:");
				outcomeParams.add(new OutcomeParameter(nameValue[0], nameValue[1]));
			}
			//b3.reportDetailedOddsTable(Long.parseLong(matchId), Long.parseLong(eventPartId), 
			//		Long.parseLong(bettingTypeId), outcomeParams);
			DetailedOddsTable2 report = new DetailedOddsTable2(
					b3, Long.parseLong(matchId), Long.parseLong(eventPartId), Long.parseLong(bettingTypeId),
					outcomeParams.toArray(new OutcomeParameter[outcomeParams.size()]));
			report.setPlainText(true);
			report.run();
			return Response.status(200).entity(new String(report.outStream.toByteArray())).build();
		}
		return Response.status(200).entity("Unknown name: " + name).build();
	}
}

abstract class Item<E> {
	
	E e;
	
	@SuppressWarnings("unchecked")
	void setEntity(Object o) {
		e = (E) o;
	}
	
	abstract String getText();
	abstract String getHref();
	String getHtml(String otherParams) {
		//return "<a href=/b3/rest/demoapp" + getHref() + otherParams + ">" + getText() + "</a><br>\n";
		return "<a href=" + getHref() + otherParams + ">" + getText() + "</a><br>\n";
	}
}

class ItemSport extends Item<Sport> {

	@Override
	String getText() {
		return e.getName();
	}

	@Override
	String getHref() {
		return "set?name=sportId&value=" + e.getId();
	}
	
}

class ItemCountry extends Item<Location> {

	@Override
	String getText() {
		return e.getName();
	}

	@Override
	String getHref() {
		return "set?name=countryId&value=" + e.getId();
	}
	
}

class ItemLeague extends Item<Event> {

	@Override
	String getText() {
		return e.getName();
	}

	@Override
	String getHref() {
		return "set?name=leagueId&value=" + e.getId();
	}
	
}

class ItemMatch extends Item<Event> {

	@Override
	String getText() {
		return e.getId() + ": startTime: " + e.getStartTime() + ", isComplete: " + e.getIsComplete();
	}

	@Override
	String getHref() {
		return "set?name=matchId&value=" + e.getId();
	}
	
}

class ItemBettingType extends Item<BettingType> {

	@Override
	String getText() {
		if (e == null) {
			return "x";
		}
		return e.getName();
	}

	@Override
	String getHref() {
		if (e == null) {
			return "x";
		}
		return "set?name=bettingTypeId&value=" + e.getId();
	}
	
}

class ItemEventPart extends Item<EventPart> {

	@Override
	String getText() {
		return e.getName();
	}

	@Override
	String getHref() {
		return "set?name=eventPartId&value=" + e.getId();
	}
	
}

class ItemOutcomeParams extends Item<OutcomeParameter[]> {

	@Override
	String getText() {
		String s = "";
		for (OutcomeParameter p : e) {
			if (!"".equals(s)) {
				s += ", ";
			}
			s += p.name + "=" + p.value;
		}
		return s;
	}

	@Override
	String getHref() {
		String s = "";
		for (OutcomeParameter p : e) {
			if (!"".equals(s)) {
				s += "|";
			}
			s += p.name + ":" + p.value;
		}
		return "set?name=outcomeParams&value=" + s;
	}
	
}
