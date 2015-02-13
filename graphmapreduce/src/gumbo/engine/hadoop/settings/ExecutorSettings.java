/**
 * Created: 20 Jan 2015
 */
package gumbo.engine.hadoop.settings;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Jonny Daenen
 *
 */
public class ExecutorSettings {


	private static final Log LOG = LogFactory.getLog(ExecutorSettings.class);

	public static final String PROOF_SYMBOL = "PROOF_SYMBOL";

	public static final String guardedIdOptimizationOn = "guardedIdOptimizationOn";
	public static final String guardIdOptimizationOn = "guardIdOptimizationOn";
	public static final String guardKeepaliveOptimizationOn = "guardKeepaliveOn";
	public static final String guardTuplePointerOptimizationOn = "guardTuplePointerOptimizationOn";
	public static final String guardAsGuardedReReadOptimizationOn = "guardAsGuardedReReadOptimizationOn";

	Map<String,String> propertiesMap;

	/**
	 * 
	 */
	public ExecutorSettings() {
		propertiesMap = new HashMap<String,String>();
		loadDefaults();
		// load user settings to overwrite defaults
	}


	/**
	 * Loads the default settings.
	 */
	private void loadDefaults() {
		propertiesMap.put(guardedIdOptimizationOn, "true");  // TODO #core make toggle
		propertiesMap.put(guardIdOptimizationOn, "true");
		propertiesMap.put(guardKeepaliveOptimizationOn, "true");
		propertiesMap.put(guardTuplePointerOptimizationOn, "true");
		propertiesMap.put(guardAsGuardedReReadOptimizationOn, "true"); // TODO #core make toggle
		propertiesMap.put(PROOF_SYMBOL, "#");

	}

	/** 
	 * Looks up a property.
	 * 
	 * @param key
	 * @return
	 */
	public String getProperty(String key) {
		if (propertiesMap.containsKey(key)) {
			return propertiesMap.get(key);
		}
		else {
			LOG.warn("Key not found in settings: " + key);
			return ""; // TODO exception + log
		}
	}

	public boolean getBooleanProperty(String key) {
		if (getProperty(key) == "true")
			return true;
		else
			return false;
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

		for (String key : propertiesMap.keySet()) {
			output += key + ":" + propertiesMap.get(key) + "\n"; 
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
			propertiesMap.put(kvpair[0], kvpair[1]);
		}


	}



}
