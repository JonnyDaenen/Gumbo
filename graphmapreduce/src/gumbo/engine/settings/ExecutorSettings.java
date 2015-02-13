/**
 * Created: 20 Jan 2015
 */
package gumbo.engine.settings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Provides access to local settings.
 * 
 * @author Jonny Daenen
 *
 */
public class ExecutorSettings extends AbstractExecutorSettings {



	private static final Log LOG = LogFactory.getLog(ExecutorSettings.class);

	Map<String,String> propertiesMap;

	
	public ExecutorSettings() {
		propertiesMap = new HashMap<String,String>();
		loadDefaults();
		// load user settings to overwrite defaults
	}



	/** 
	 * Looks up a property.
	 * 
	 * @param key
	 * @return
	 */
	@Override
	public String getProperty(String key) {
		if (propertiesMap.containsKey(key)) {
			return propertiesMap.get(key);
		}
		else {
			LOG.warn("Key not found in settings: " + key);
			return ""; // TODO exception + log
		}
	}



	@Override
	public void setProperty(String key, String value) {
		propertiesMap.put(key, value);
		
	}



	@Override
	public Set<String> getAllKeys() {
		return propertiesMap.keySet();
	}
	
	


}
