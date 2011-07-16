/**
 * 
 */
package data_other;

import weka.core.*;

/**
 * @author arielstolerman
 * 
 * Uses weka classes to represent attributes and instances
 *
 */
public interface DataSet {
	
	/**
	 * Saves the dataset in an ARFF format to a given path.
	 * @param out
	 * 				Path to the output ARFF file.
	 */
	public void toArffFile(String out);
	
	// getters
	
	/**
	 * Get the list of attributes.
	 * @return
	 * 				The list of attributes.
	 */
	public Attribute[] getAttributes();
}
