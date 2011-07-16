package data_other;

import weka.core.*;
import weka.core.converters.*;
import java.io.*;

/**
 * 
 * @author arielstolerman
 * 
 * The WekaDataSet class is a weka-like data representation, which uses a weka.core.Instances to represent the instances as is.
 * This means that the whole instances data is saved in the memory, like in weka itself.
 * This dataset type will not work well on large datasets with relatively little memory resources.
 *
 */
public class WekaDataSet extends BasicDataSet {

	/* ******
	 * fields
	 * ******/
	
	protected Instances instances;
	
	/* ************
	 * constructors
	 * ************/
	
	/**
	 * 
	 * @param attrs
	 */
	public WekaDataSet(Attribute[] attrs) {
		super(attrs);
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * 
	 * @param attrs
	 * @param instances
	 */
	public WekaDataSet(Attribute[] attrs, Instances instances) {
		super(attrs);
		this.instances = instances;
	}
	
	/* ********
	 * creators
	 * ********/

	// ARFF handling

	@Override
	public void toArffFile(String out) throws IOException {
		ArffSaver saver = new ArffSaver();
		saver.setInstances(instances);
		saver.setFile(new File(out));
		//saver.setDestination(new File(out)); // -- unnecessary for weka 3.5.4 and later
		saver.writeBatch();
	}

	@Override
	public void toSparseArffFile(String out) throws IOException {
		toArffFile(out);
	}
	
	/* *********
	 * modifiers
	 * *********/
	
	/**
	 * Sets the Instances data to the given Instances.
	 * @param instances
	 * 				The new Instances data to be set.
	 */
	public void setInstances(Instances instances) {
		this.instances = instances;
	}

	/* *******
	 * queries
	 * *******/

	// basic getters

	/**
	 * Gets the Instances data.
	 * @return
	 * 				The Instances data.
	 */
	public Instances getInstances() {
		return instances;
	}


}
