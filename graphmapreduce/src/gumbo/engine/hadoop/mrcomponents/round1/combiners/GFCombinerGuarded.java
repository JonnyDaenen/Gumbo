package gumbo.engine.hadoop.mrcomponents.round1.combiners;

import gumbo.engine.hadoop.mrcomponents.tools.ParameterPasser;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GFCombinerGuarded extends Reducer<Text, Text, Text, Text> {
	

	private static final Log LOG = LogFactory.getLog(GFCombinerGuarded.class);

	private ExpressionSetOperations eso;
	private HadoopExecutorSettings settings;

	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// load context
		super.setup(context);
		Configuration conf = context.getConfiguration();


		// load parameters
		try {
			ParameterPasser pp = new ParameterPasser(conf);
			eso = pp.loadESO();
			settings = pp.loadSettings();
		} catch (Exception e) {
			throw new InterruptedException("Combiner initialisation error: " + e.getMessage());
		}
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		
		String assertMsg = "";
		
		// loop through given values
		for (Text t : values) {
			String value = t.toString();
//			System.out.println(value);
			
			// only continue if value is an ASSERT message, i.e: contains no ';' 
			if (!value.contains(";")) {
				assertMsg += ":" + value.substring(2);
			}
			// if it contains ';', just output
			else {
				context.write(key, t);
			}
		}
		
		if (assertMsg.length() > 0) {
			context.write(key, new Text("#"+assertMsg));
		}
			
		
	}

}
