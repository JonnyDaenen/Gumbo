/**
 * Created: 20 Jan 2015
 */
package mapreduce.guardedfragment.executor.hadoop;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jonny Daenen
 *
 */
public class ExecutorSettings {
	
	public static final String PROOF_SYMBOL = "PROOF_SYMBOL";
	
	public static final String guardedIdOptimizationOn = "guardedIdOptimizationOn";
	public static final String guardIdOptimizationOn = "guardIdOptimizationOn";
	public static final String guardKeepaliveOptimizationOn = "guardKeepaliveOn";
	public static final String guardTuplePointerOptimizationOn = "guardTuplePointerOptimizationOn";
	
	Map<String,String> propertiesMap;
	
	/**
	 * 
	 */
	public ExecutorSettings() {
		propertiesMap = new HashMap<String,String>();
		loadDefaults();
	}
	
	
	/**
	 * 
	 */
	private void loadDefaults() {
		propertiesMap.put(guardedIdOptimizationOn, "false");
		propertiesMap.put(guardIdOptimizationOn, "true");
		propertiesMap.put(guardKeepaliveOptimizationOn, "false");
		propertiesMap.put(guardTuplePointerOptimizationOn, "false");
		propertiesMap.put(PROOF_SYMBOL, "#");
		
	}


	public String getProperty(String key) {
		if (propertiesMap.containsKey(key)) {
			return propertiesMap.get(key);
		}
		else {
			return ""; // TODO exception + log
		}
	}
	
	public boolean getBooleanProperty(String key) {
		if (getProperty(key) == "true")
			return true;
		else
			return false;
	}
	
	
	

}
