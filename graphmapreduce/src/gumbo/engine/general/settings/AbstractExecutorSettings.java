/**
 * Created: 13 Feb 2015
 */
package gumbo.engine.general.settings;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.partitioner.HeightPartitioner;
import gumbo.engine.general.grouper.GroupingPolicies;



/**
 * Provides access to settings for the gumbo framework.
 * 
 * @author Jonny Daenen
 *
 */
public abstract class AbstractExecutorSettings {



	private static final Log LOG = LogFactory.getLog(AbstractExecutorSettings.class);

	

	// compression options
	public static final String assertConstantOptimizationOn = "gumbo.engine.assertConstantOptimizationOn";
	public static final String requestAtomIdOptimizationOn = "gumbo.engine.requestAtomIdOptimizationOn";
	public static final String guardKeepAliveOptimizationOn = "gumbo.engine.guardKeepAliveOptimizationOn";
	public static final String guardReferenceOptimizationOn = "gumbo.engine.guardAddressOptimizationOn";
//	public static final String guardAsGuardedReReadOptimizationOn = "gumbo.engine.guardAsGuardedReReadOptimizationOn"; // TODO this is currently implemented, but needs to become a toggle
	public static final String mapOutputGroupingOptimizationOn = "gumbo.engine.mapOutputGroupingOptimizationOn";
	public static final String reduceOutputGroupingOptimizationOn = "gumbo.engine.reduceOutputGroupingOptimizationOn";	  

	// speed options
	public static final String guardedCombinerOptimizationOn = "gumbo.engine.guardedCombinerOptimizationOn";
	public static final String round1FiniteMemoryOptimizationOn = "gumbo.engine.round1FiniteMemoryOptimizationOn"; 
	
	public static final String turnOffOpts = "gumbo.engine.turnOffOpts";
	public static final String turnOnOpts = "gumbo.engine.turnOnOpts";
	public static final String turnOnDefaultOpts = "gumbo.engine.turnOnDefaultOpts";

	public static final String PROOF_SYMBOL = "gumbo.engine.proofsymbol";
	public static final String partitionClass = "gumbo.compiler.partitioner";
	public static final String mapOutputGroupingPolicy = "gumbo.engine.mapOutputGroupingPolicy";

	// engine v2 options
	public static final String UNNEST_ON = "gumbo.compiler.unnest";
	
	public static final String SIMULATOR_CLASS = "gumbo.engine.simulator.classname";
	public static final String VAL_PREPACK = "gumbo.engine.val.projection.prepack";
	public static final String EVAL_OUTMERGE = "gumbo.engine.eval.output.merge";
	public static final String VALEVAL_PREPACK = "gumbo.engine.valeval.projection.prepack";
	public static final String VALEVAL_ON = "gumbo.engine.valeval.enabled";
	
	// constants
	public static final String REDUCER_SIZE_MB = "gumbo.engine.hadoop.reducersize_mb";
	public static final String BESTGROUP_STOPINDICATOR = "gumbo.engine.grouper.beststopindicator";












	

	/**
	 * Loads the default settings.
	 */
	public void loadDefaults() {
		turnOnOptimizations();
		setBooleanProperty(guardedCombinerOptimizationOn, false); 
		setBooleanProperty(round1FiniteMemoryOptimizationOn, false); 
//		setBooleanProperty(mapOutputGroupingOptimizationOn, false); 
		
		setProperty(PROOF_SYMBOL, "#");
		setProperty(partitionClass, HeightPartitioner.class.getCanonicalName());
		setProperty(mapOutputGroupingPolicy, GroupingPolicies.COSTGROUP_GUMBO.toString());
//		setProperty(mapOutputGroupingClass, AllGrouper.class.getCanonicalName());
		
		setProperty(REDUCER_SIZE_MB, "1024");
		
		// engine v2
		setBooleanProperty(UNNEST_ON, false); 
		setBooleanProperty(VAL_PREPACK, true); 
		setBooleanProperty(EVAL_OUTMERGE, false); 
		setBooleanProperty(VALEVAL_PREPACK, true); 
		setBooleanProperty(VALEVAL_ON, true); 
//		setProperty(simulatorClass, Simulator.class.getCanonicalName());
		setProperty(SIMULATOR_CLASS, gumbo.engine.hadoop2.estimation.MapSimulator.class.getCanonicalName());
	}
	
	public void turnOffOptimizations() {
		setBooleanProperty(assertConstantOptimizationOn, false);
		setBooleanProperty(requestAtomIdOptimizationOn, false);
		setBooleanProperty(guardKeepAliveOptimizationOn, false);
		setBooleanProperty(guardReferenceOptimizationOn, false);
		setBooleanProperty(round1FiniteMemoryOptimizationOn, false); 
		setBooleanProperty(guardedCombinerOptimizationOn, false); 
		setBooleanProperty(mapOutputGroupingOptimizationOn, false); 
		setBooleanProperty(reduceOutputGroupingOptimizationOn, false); 
	}
	
	public void turnOnOptimizations() {
		setBooleanProperty(assertConstantOptimizationOn, true);
		setBooleanProperty(requestAtomIdOptimizationOn, true);
		setBooleanProperty(guardKeepAliveOptimizationOn, true);
		setBooleanProperty(guardReferenceOptimizationOn, true);
		setBooleanProperty(round1FiniteMemoryOptimizationOn, true);
		setBooleanProperty(guardedCombinerOptimizationOn, true);
		setBooleanProperty(mapOutputGroupingOptimizationOn, true); 
		setBooleanProperty(reduceOutputGroupingOptimizationOn, true); 

	}
	

	public static Set<String> getAllKeys() {
		HashSet<String> keys = new HashSet<>();
		
	    Field[] allFields = AbstractExecutorSettings.class.getDeclaredFields();
	    for (Field field : allFields) {
	    	if (field.getName().toLowerCase().contains("optimization")) {
	    		try {
					keys.add((String) field.get(null));
				} catch (Exception e) {
					LOG.warn(e.getMessage());
				}
	    	}
	    }
		return keys;
	}

	/** 
	 * Looks up a property.
	 */
	public abstract String getProperty(String key);

	public abstract void setProperty(String key, String value);

	public boolean getBooleanProperty(String key) {
		if (getProperty(key).toLowerCase().equals("true"))
			return true;
		else
			return false;
	}


	public void setBooleanProperty(String key, boolean value) {
		if (value)
			setProperty(key,"true");
		else
			setProperty(key,"false");
	}

	@Override
	public String toString() {
		return saveToString();
	}


	/**
	 * Encodes the settings as a string.
	 * @return an encoded settings string
	 */
	public String saveToString() {
		String output = "";

		for (String key : getAllKeys()) {
			output += key + ":" + getProperty(key) + "\n"; 
		}
		return output;
	}

	/**
	 * Adds settings, overwriting existing ones,
	 * bad formatted entries are skipped.
	 * 
	 * @param s encoded settings string
	 */
	public void loadFromString(String s) {
		String [] lines = s.split("\n");
		for (String line : lines) {
			String[] kvpair = line.split("\n", 2);
			if (kvpair.length < 2 || kvpair[0] == null || kvpair[1] == null) {
				LOG.warn("skipping badly formatted setting: " + line);
				continue;
			}
			setProperty(kvpair[0], kvpair[1]);
		}


	}
	

	public String save() {
		String output = "";
		for (String key : getAllKeys()) {
			String value = getProperty(key);
			output += key + "=" + value + ";";
		}
		return output;
	}

	
	public void load(String input) {
		String [] pairs = input.split(";");
		for (String pair : pairs) {
			String [] kv = pair.split("=");
			if (kv.length != 2)
				continue;
			String key = kv[0];
			String value = kv[1];
			setProperty(key, value);
		}
	}
	
	public double getNumProperty(String key, double defaultValue) {
		try {
			return getNumProperty(key);
		} catch (Exception e) {
			LOG.warn("Failed to find setting with key '"+key+"', reverting to default value " + defaultValue);
			return defaultValue;
		}
	}

	public double getNumProperty(String key) {
		String val = getProperty(key);
		return Double.parseDouble(val);
	}


}
