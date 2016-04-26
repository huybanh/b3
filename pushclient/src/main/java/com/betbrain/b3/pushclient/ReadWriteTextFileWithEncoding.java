package com.betbrain.b3.pushclient;

import java.io.*;
import java.util.Scanner;

/** 
 Read and write a file using an explicit encoding.
 JDK 1.5.
 Removing the encoding from this code will simply cause the 
 system's default encoding to be used instead.  
*/


public final class ReadWriteTextFileWithEncoding {

	  /** Requires two arguments - the file name, and the encoding to use.  */
	  public static void main(String... aArgs) throws IOException {
	    String fileName = aArgs[0];
	    String encoding = aArgs[1];
	    ReadWriteTextFileWithEncoding test = new ReadWriteTextFileWithEncoding(
	      fileName, encoding, "");
	    test.write();
	    test.read();
	  }
	  
	  /** Constructor. */
	  ReadWriteTextFileWithEncoding(String aFileName, String aEncoding, String sText){
	    fEncoding = aEncoding;
	    fFileName = aFileName;
	    if(sText == null || sText == "") {
	    	setsOutput(FIXED_TEXT);
	    }
	    else {
	    	setsOutput(sText);
	    }
	  }
	  
	  
	  ReadWriteTextFileWithEncoding(String aFileName, String aEncoding) {
		  fEncoding = aEncoding;
		  fFileName = aFileName;
	  }
		 
	  
	  /** Write fixed content to the given file. */
	  public void write() throws IOException  {
	    log("Writing to file named " + fFileName + ". Encoding: " + fEncoding);
	    Writer out = new OutputStreamWriter(new FileOutputStream(fFileName, true), fEncoding);
	   
	    try {
	      out.write(getsOutput());
	    }
	    finally {
	      out.close();
	    }
	  }
	  
	  /** Read the contents of the given file. */
	  public void read() throws IOException {
	    log("Reading from file.");
	    StringBuilder text = new StringBuilder();
	    String NL = System.getProperty("line.separator");
	    Scanner scanner = new Scanner(new FileInputStream(fFileName), fEncoding);
	    try {
	      while (scanner.hasNextLine()){
	        text.append(scanner.nextLine() + NL);
	      }
	    }
	    finally{
	      scanner.close();
	    }
	    log("Text read in: " + text);
	  }
	  
	  // PRIVATE 
	  private final String fFileName;
	  private final String fEncoding;
	  private String sOutput;
	  private final String FIXED_TEXT = "But soft! what code in yonder program breaks?";
	  
	  private void log(String aMessage){
	    System.out.println(aMessage);
	  }

	/**
	 * @return the sOutput
	 */
	public String getsOutput() {
		return sOutput;
	}
	
	/**
	 * @param sOutput the sOutput to set
	 */
	public void setsOutput(String sOutput) {
		this.sOutput = sOutput;
	}
}