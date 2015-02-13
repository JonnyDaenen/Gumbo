/**
 * Created: 13 May 2014
 */
package gumbo.compiler.resolver.reducers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * Create output files based on the output record's key name. based on:
 * http://grepalex.com/2013/05/20/multipleoutputs-part1/
 * 
 * @author Jonny Daenen
 * 
 * 
 */
public class KeyBasedMultipleTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {

	private static final Log LOG = LogFactory.getLog(KeyBasedMultipleTextOutputFormat.class);
	
	/**
	 * 
	 */
	public KeyBasedMultipleTextOutputFormat() {
		LOG.error("output object created.");
	}
	
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value, String name) {

		LOG.error("request for " + key);
		return key.toString() + "/" + name;
	}
	
}
