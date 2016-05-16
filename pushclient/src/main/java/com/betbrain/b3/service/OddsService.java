package com.betbrain.b3.service;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.b3.report.detailedodds.DetailedOddsTable2;
 
@Path("/odds")
public class OddsService {
	
	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();
		new OddsService().detailedOddsTable(217633296, 3, true, "text");
	}
	
	@GET
	@Path("/detailedtable/{matchid}")
	@Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
	public Response detailedOddsTable(@PathParam("matchid") long matchId, 
			@DefaultValue("3") @QueryParam("eventPartId") long eventPartId,
			@DefaultValue("false") @QueryParam("prettyPrint") boolean prettyPrint,
			//@HeaderParam("Accept") String accepted) {
			@QueryParam("format") String format) {

		boolean plainText = false;
		/*if (accepted != null && !"".equals(accepted.trim())) {
			plainText = MediaType.valueOf(accepted) == MediaType.TEXT_PLAIN_TYPE;
		}*/
		if (format != null && "text".equalsIgnoreCase(format)) {
			plainText = true;
		}
		JsonMapper mapper = new JsonMapper();
		DetailedOddsTable2 report = new DetailedOddsTable2(matchId, eventPartId, plainText, mapper);
		report.run();
		
		if (!plainText) {
			//LinkedList<DetailedOddsTableData> reportData = report.outputData;
			//return reportData.toArray(new DetailedOddsTableData[reportData.size()]);
			mapper.prettyPrint(prettyPrint);
			return Response.status(200).entity(mapper.deepSerialize(report.outputData)).build();
		} else {
			return Response.status(200).entity(new String(report.outStream.toByteArray())).build();
		}
	}
 
}