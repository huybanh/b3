package com.betbrain.b3.report;

public class IDs {

	public static final long SPORT_FOOTBALL = 1;
	
	public static final long EVENTTYPE_GENERICMATCH = 1;
	public static final long EVENTTYPE_GENERICTOURNAMENT = 2;
	
	public static final long EVENTPART_ORDINARYTIME =3;

	public static final long EVENTINFOTYPE_SCORE = 1;
	public static final long EVENTINFOTYPE_CURRENTSTATUS = 92;
	
	//public static final long EVENT_PREMIERLEAGUE = 215458667;
	public static final long EVENT_PREMIERLEAGUE = 215754838;
	
	public static final long OUTCOMETYPE_DRAW = 11;
	public static final long OUTCOMETYPE_WINNER = 10;
	
	public static final long BETTINGTYPE_1X2 = 69;
	
	public static void main(String[] args) {
		System.out.println(new Integer(12345).hashCode());
		System.out.println(new Integer(12346).hashCode());
		System.out.println("12345".hashCode());
		System.out.println("12346".hashCode());
		System.out.println("a12345".hashCode());
		System.out.println("a12346".hashCode());
	}
}
