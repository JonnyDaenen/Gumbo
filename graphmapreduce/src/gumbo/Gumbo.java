/**
 * Created: 09 Feb 2015
 */
package gumbo;

import gumbo.compiler.GFCompiler;
import gumbo.compiler.GumboPlan;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.partitioner.CalculationPartitioner;
import gumbo.engine.general.grouper.Grouper;
import gumbo.engine.general.grouper.GrouperFactory;
import gumbo.engine.general.grouper.costmodel.GumboCostModel;
import gumbo.engine.general.grouper.costmodel.IOCostModel;
import gumbo.engine.general.grouper.costmodel.MRSettings;
import gumbo.engine.general.grouper.costmodel.PaperCostModel;
import gumbo.engine.general.grouper.policies.CostBasedGrouper;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.general.utils.FileMappingExtractor;
import gumbo.engine.hadoop.HadoopEngine;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.hadoop2.HadoopEngine2;
import gumbo.input.GumboFileParser;
import gumbo.input.GumboQuery;

import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
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
		
		Thread.sleep(5000);

		String tmpdir = getConf().get("java.io.tmpdir");
		if (tmpdir != null)
			System.setProperty("java.io.tmpdir", tmpdir);
//		System.out.println(System.getProperty("java.io.tmpdir"));


		// load Configuration processed by ToolRunner
		HadoopExecutorSettings settings = new HadoopExecutorSettings();
		settings.loadDefaults();

		if (getConf().getBoolean(AbstractExecutorSettings.turnOffOpts, false))
			settings.turnOffOptimizations();

		if (getConf().getBoolean(AbstractExecutorSettings.turnOnOpts, false))
			settings.turnOnOptimizations();


		settings.loadConfig(getConf());
		//		settings.setBooleanProperty(AbstractExecutorSettings.round1FiniteMemoryOptimizationOn, false);


		for (Entry<String, String> entry: settings.getConf()) {
			if ( entry.getKey().contains("gumbo")) {
				System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
			}
		}

		// get file argument
		// FUTURE make more advanced
		String filename = null;
		boolean pickup = false;
		boolean pickupname = false;
		boolean groupOnly = false;
		String jobname = null;

		for (String arg : args) {
			if (arg.equals("-f")) {
				pickup = true;
			} else if (pickup) {
				filename = arg;
				pickup = false;
			}
			else if (arg.equals("--grouponly")) {
				groupOnly = true;
			}
			else if (arg.equals("-j")) {
				pickupname = true;
			} else if (pickupname) {
				jobname = arg;
				pickupname = false;
			}
			else if (arg.equals("--infix")) {
				// FUTURE implement
			}
		}

		if (filename == null) {
			throw new NoQueryException("No query file given.");
		}

		// parse file
		GumboFileParser parser = new GumboFileParser();
		//		parser.setInfix(infix); TODO 
		//		GumboScriptFileParser parser = new GumboScriptFileParser();
		GumboQuery query = parser.parse(filename, jobname);

		LOG.info(query);


		CalculationPartitioner partitioner;
		String partClassName = settings.getProperty(AbstractExecutorSettings.partitionClass);
		partitioner = (CalculationPartitioner) this.getClass().getClassLoader().loadClass(partClassName).newInstance();

		GFCompiler compiler = new GFCompiler(partitioner);
		GumboPlan plan = compiler.createPlan(query);

		System.out.println(plan);


		if (groupOnly) {
			
			long start = System.nanoTime();
			FileMappingExtractor fme = new FileMappingExtractor(false);
			RelationFileMapping mapping2 = fme.extractFileMapping(plan.getFileManager());
			Grouper grouper = GrouperFactory.createGrouper(mapping2, settings, null);
			grouper.group(plan.getPartitions().getPartition(0));
//
			System.out.println((System.nanoTime() - start)/(1000000000.0D));
			System.exit(0);
		} else {

			HadoopEngine2 engine = new HadoopEngine2();
			engine.executePlan(plan,settings.getConf());

//			System.out.println(engine.getCounters());
		}
		return 0;
		
	}


	public static void main(String[] args) throws Exception {
		//		Thread.sleep(10000);
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new Gumbo(), args);

		System.exit(res);
	}



}
