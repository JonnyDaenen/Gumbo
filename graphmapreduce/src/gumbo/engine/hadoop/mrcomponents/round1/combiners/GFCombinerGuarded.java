package gumbo.engine.hadoop.mrcomponents.round1.combiners;

import gumbo.engine.hadoop.mrcomponents.tools.ParameterPasser;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

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

		boolean keyFound = false;
		
		// loop through given values
		for (Text t : values) {
			
			// only continue if value is a proof of existence, i.e: contains no ';' 
			if (!t.toString().contains(";")) {
				// check if it has been output already
				if (!keyFound) {
					keyFound = true;
					context.write(key, t);
				}
			}
			// if it contains ';', just output
			else {
				context.write(key, t);
			}
		}
		
	}

}
