/**
 * Created: 13 Feb 2015
 */
package gumbo.engine.settings;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/**
 * Provides access to settings for the gumbo framework.
 * 
 * @author Jonny Daenen
 *
 */
public abstract class AbstractExecutorSettings {



	private static final Log LOG = LogFactory.getLog(AbstractExecutorSettings.class);

	public static final String PROOF_SYMBOL = "gumbo.engine.proofsymbol";

	// compression options
	public static final String assertConstantOptimizationOn = "gumbo.engine.assertConstantOptimizationOn";
	public static final String atomIdOptimizationOn = "gumbo.engine.requestAtomIdOptimizationOn"; // TODO this is always on now
	public static final String guardKeepAliveReductionOn = "gumbo.engine.guardKeepAliveReductionOn";
	public static final String guardReferenceOptimizationOn = "gumbo.engine.guardAddressOptimizationOn";
//	public static final String guardAsGuardedReReadOptimizationOn = "gumbo.engine.guardAsGuardedReReadOptimizationOn"; // TODO this is currently implemented, but needs to become a toggle

	public static final String guardedCombinerOptimizationOn = "gumbo.engine.guardedCombinerOptimizationOn";  

	// speed options
	public static final String round1FiniteMemoryOptimizationOn = "gumbo.engine.round1FiniteMemoryOptimizationOn"; 
	

	/**
	 * Loads the default settings.
	 */
	public void loadDefaults() {
		turnOffOptimizations();
		setProperty(PROOF_SYMBOL, "#");
	}
	
	public void turnOffOptimizations() {
		setBooleanProperty(assertConstantOptimizationOn, false);
		setBooleanProperty(atomIdOptimizationOn, false);
		setBooleanProperty(guardKeepAliveReductionOn, false);
		setBooleanProperty(guardReferenceOptimizationOn, false);
//		setBooleanProperty(guardAsGuardedReReadOptimizationOn, false); 
		setBooleanProperty(round1FiniteMemoryOptimizationOn, false); 
		setBooleanProperty(guardedCombinerOptimizationOn, false); 
	}
	
	public void turnOnOptimizations() {
		setBooleanProperty(assertConstantOptimizationOn, true);
		setBooleanProperty(atomIdOptimizationOn, true);
		setBooleanProperty(guardKeepAliveReductionOn, true);
		setBooleanProperty(guardReferenceOptimizationOn, true);
//		setBooleanProperty(guardAsGuardedReReadOptimizationOn, true);
		setBooleanProperty(round1FiniteMemoryOptimizationOn, true);
		setBooleanProperty(guardedCombinerOptimizationOn, true);

	}
	

	public static Set<String> getAllKeys() {
		HashSet<String> keys = new HashSet<>();
		keys.add(assertConstantOptimizationOn);
		keys.add(atomIdOptimizationOn);
		keys.add(guardKeepAliveReductionOn);
		keys.add(guardReferenceOptimizationOn);
//		keys.add(guardAsGuardedReReadOptimizationOn);
		keys.add(round1FiniteMemoryOptimizationOn);
		keys.add(guardedCombinerOptimizationOn);
		// CLEAN use reflection
		return keys;
	}

	/** 
	 * Looks up a property.
	 */
	public abstract String getProperty(String key);

	public abstract void setProperty(String key, String value);

	public boolean getBooleanProperty(String key) {
		if (getProperty(key).equals("true"))
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


}
