/**
 * Created: 09 Feb 2015
 */
package gumbo;

import gumbo.compiler.GFCompiler;
import gumbo.compiler.GumboPlan;
import gumbo.engine.hadoop.HadoopEngine;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.input.GumboFileParser;
import gumbo.input.GumboScriptFileParser;
import gumbo.input.GumboQuery;

import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author Jonny Daenen
 *
 */
public class Gumbo extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(Gumbo.class);

	public class NoQueryException extends Exception {
		private static final long serialVersionUID = 1L;

		public NoQueryException(String msg) {
			super(msg);
		}

	}


	public int run(String[] args) throws Exception {
		// Configuration processed by ToolRunner
		Configuration conf = getConf(); 
		
		HadoopExecutorSettings settings = new HadoopExecutorSettings(conf);
		settings.loadDefaults();
		settings.turnOffOptimizations();
		
		for (Entry<String, String> entry: conf) {
			if(entry.getKey().contains("gumbo")) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
			}
		}

		// get file argument
		// FUTURE make more advanced
		String filename = null;
		boolean pickup = false;
		for (String arg : args) {
			if (arg.equals("-f")) {
				pickup = true;
			} else if (pickup) {
				filename = arg;
			}
		}
		
		if (filename == null) {
			throw new NoQueryException("No query file given.");
		}
		
		// parse file
		//GumboFileParser parser = new GumboFileParser();
		GumboScriptFileParser parser = new GumboScriptFileParser();
		GumboQuery query = parser.parse(filename);
		
		LOG.info(query);
		
		GFCompiler compiler = new GFCompiler();
		GumboPlan plan = compiler.createPlan(query);

		System.out.println(plan);

		HadoopEngine engine = new HadoopEngine();
		engine.executePlan(plan,conf);

		return 0;
	}


	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new Gumbo(), args);

		System.exit(res);
	}



}
