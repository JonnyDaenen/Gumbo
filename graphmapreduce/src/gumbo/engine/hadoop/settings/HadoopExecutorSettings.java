/**
 * Created: 20 Jan 2015
 */
package gumbo.engine.hadoop.settings;

import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import gumbo.engine.general.settings.AbstractExecutorSettings;

/**
 * Provides access to settings for the gumbo framework in a Hadoop {@link Configuration}.
 * 
 * @author Jonny Daenen
 *
 */
public class HadoopExecutorSettings extends AbstractExecutorSettings{


	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(HadoopExecutorSettings.class);
	
	private Configuration conf;

	
	
	public HadoopExecutorSettings() {
		this.conf = new Configuration();
	}
	
	public HadoopExecutorSettings(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public String getProperty(String key) {
		String result = conf.get(key);
		if (result == null)
			result = "";
		return result;
	}

	@Override
	public void setProperty(String key, String value) {
//		System.out.println(key);
		conf.set(key, value);
	}

	public Configuration getConf() {
		return conf;
	}
	
	/**
	 * copies the configuration in the current settings.
	 * Settings that are already present are overwritten.
	 * @param conf the configuration to load
	 */
	public void loadConfig(Configuration conf) {
		
		for (Entry<String, String> a : conf) {
			setProperty(a.getKey(), a.getValue());
		}
	}



	


}
