package com.betbrain.b3.service;
 
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.report.oddsdetailed.DetailedOddsTable;
 
@Path("/rtr")
public class RealTimeReport {
 
	@GET
	@Path("/{matchid}")
	public Response getMsg(@PathParam("matchid") String msg) {
 
		//DynamoWorker.initBundleCurrent();
		DynamoWorker.initBundleByStatus("SPRINT2");
		String sReport = (new DetailedOddsTable(Long.parseLong(msg))).run();
		return Response.status(200).entity(sReport).build();
 
	}
 
}