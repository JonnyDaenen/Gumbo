/**
 * Created: 20 Jan 2015
 */
package gumbo.engine.hadoop.settings;

import gumbo.engine.settings.AbstractExecutorSettings;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

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
		conf.set(key, value);	
	}

	public Configuration getConf() {
		return conf;
	}



	


}
