package data_other;

import weka.core.*;
import java.util.*;

/**
 * 
 * @author arielstolerman
 *
 * Basic representation of a dynamic network, consisting of transactors and transactions characterized by a timestamp and 
 * weight (e.g. duration, number of connections in the timestamp etc.).
 * 
 */
public class DynamicNetworkDataSet {
	
	// constant
	
	public int DYNAMIC_NETWORK_NUM_OF_ATTRS = 4;
	
	// fields
	
	protected Attribute[] attrs;
	
	// constructors
	
	/**
	 * Constructor for a basic representation of a dynamic network.
	 * @param dateFormat
	 * 				The date format for the timestamp attribute.
	 */
	public DynamicNetworkDataSet(String dateFormat) {
		// initialize attributes
		initAttrs(dateFormat);
	}
	
	// initialization methods
	
	/**
	 * Initializes the attributes to timestamp, sender transactor, recipient transactor and weight.
	 * @param dateFormat
	 * 				The date format for the timestamp.
	 */
	protected void initAttrs(String dateFormat) {
		attrs = new Attribute[DYNAMIC_NETWORK_NUM_OF_ATTRS];
		
		List<String> nullList = null;
		
		// timestamp
		attrs[0] = new Attribute("timestamp",dateFormat);
		
		// sender
		attrs[1] = new Attribute("sender",nullList);
		
		// recipient
		attrs[2] = new Attribute("recipient",nullList);
		
		// weight
		attrs[3] = new Attribute("weight");
		
	}
}
