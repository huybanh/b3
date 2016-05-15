package com.betbrain.b3.model;

import java.util.ArrayList;

public class DetailsOddEntity {

	private ArrayList<DetailsOddPartEntity> dataReport;

	public ArrayList<DetailsOddPartEntity> getDataReport() {
		return dataReport;
	}

	public void setDataReport(ArrayList<DetailsOddPartEntity> dataReport) {
		this.dataReport = dataReport;
	}
	
	public DetailsOddEntity() {
		dataReport = new ArrayList<>();
	}
}
