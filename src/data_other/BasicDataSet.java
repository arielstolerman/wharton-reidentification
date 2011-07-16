package data_other;

import weka.core.*;
import java.io.*;

/**
 * 
 * @author arielstolerman
 *
 * The BasicDataSet abstract class represents the lowest common factors between all datasets.
 * The datasets rely on the weka.core api for the attributes and instances representation.
 * The attributes are save in the memory throughout the entire life of the dataset, but the Instances may be saved in different
 * methods, like saving all in the memory, or saving several Instances on disk and fetching them to memory when used.
 * 
 */
public abstract class BasicDataSet {

	Attribute[] attrs;

	/* ************
	 * constructors
	 * ************/

	/**
	 * Constructor for a basic dataset. Sets the attribute list to the given attributes.
	 * @param attrs
	 * 				The list of dataset attributes to represent the data.
	 */
	public BasicDataSet(Attribute[] attrs) {
		this.attrs = attrs;
	}

	/* ********
	 * creators
	 * ********/

	// ARFF handling

	/**
	 * Saves the dataset in an ARFF format to a given path.
	 * @param out
	 * 				Path to the output ARFF file.
	 */
	public abstract void toArffFile(String out) throws IOException;

	/**
	 * Saves the dataset in a sparse ARFF format to a given path if possible. Otherwise it will be saved in a regular ARFF format. 
	 * @param out
	 * 				Path to the output ARFF file.
	 */
	public abstract void toSparseArffFile(String out) throws IOException;

	/* *********
	 * modifiers
	 * *********/

	/* *******
	 * queries
	 * *******/

	// basic getters

	/**
	 * Gets the list of attributes.
	 * @return
	 * 				The list of attributes.
	 */
	public Attribute[] getAttributes() {
		return attrs;
	}

}
