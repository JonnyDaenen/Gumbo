/**
 * Created: 09 Oct 2014
 */
package mapreduce.guardedfragment.executor.hadoop.combiners;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.executor.hadoop.reducers.GFReducer1;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.ExpressionSetOperations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author jonny
 *
 */
public class GFCombiner1 extends Reducer<Text,Text,Text,Text> {
	

	String FILENAME = "tmp_round1_comb.txt";
	
	private static final Log LOG = LogFactory.getLog(GFCombiner1.class);
	
	protected Text out1 = new Text();
	protected Text out2 = new Text();
	private ExpressionSetOperations eso;
	protected MultipleOutputs<Text, Text> mos;


	StringBuilder sb;

	boolean outputIDs = true;

	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// load context
		super.setup(context);
		Configuration conf = context.getConfiguration();
		
		String s = String.format("map-output-%05d-%d",
		        context.getTaskAttemptID().getTaskID().getId(),
		        context.getTaskAttemptID().getId());
		LOG.info(s);

		mos = new MultipleOutputs<Text, Text>(context);
		sb = new StringBuilder(100);

		GFPrefixSerializer serializer = new GFPrefixSerializer();

		// load guard
		try {
			HashSet<GFExistentialExpression> formulaSet = new HashSet<GFExistentialExpression>();
			String formulaString = conf.get("formulaset");
			Set<GFExpression> deserSet = serializer.deserializeSet(formulaString);

			// check whether the type is existential
			// FUTURE allow other types?
			for (GFExpression exp : deserSet) {
				if (exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}

			eso = new ExpressionSetOperations();
			eso.setExpressionSet(formulaSet);

		} catch (Exception e) {
			throw new InterruptedException("Reducer initialisation error: " + e.getMessage());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Set<Pair<String, Integer>> buffer = new HashSet<>();

//		LOG.warn("Combining: " + key);

		boolean keyFound = false;

		// WARNING Text object will be reused by Hadoop!
		for (Text value : values) {

			// parse input
			Pair<String, Integer> split = split(value);
			
			// is this not the key
			// (key is only thing that can appear without atom)
			if (split.snd != -1) {


				// if the key has already been found, we can just output
				if (keyFound) {
					out1.set(split.fst);
					out2.set(""+split.snd);
					mos.write(out1, out2, FILENAME);
				}
				// else we collect the data
				else {
					buffer.add(split);
				}

				// if this is the key, we mark it and propagate it
			} else if (!keyFound) {
				keyFound = true;
				context.write(key, value);
			}
		}

		// output the remaining data
		if (keyFound) {
			for (Pair<String, Integer> p : buffer) {
				out1.set(p.fst);
				out2.set(""+p.snd);
				mos.write(out1, out2, FILENAME);
			}
		} else {
			for (Pair<String, Integer> p : buffer) {
				out2.set(p.fst + ";" + p.snd);
				context.write(key, out2);
			}
		}

	}
	
	/**
	 * Splits Stirng into 2 parts. String is supposed to be separated with ';'.
	 * When no ';' is present, the numeric value is -1. 
	 * @param t
	 */
	protected Pair<String, Integer> split(Text t) {
		int length = t.getLength();
		String output = null;
		int num = -1;
		sb.setLength(0);
		boolean numberPart = false;

		byte[] b = t.getBytes();
		for (int i = 0; i < length; i++) { // FUTURE for unicode this doesn't
											// work I guess..
			char c = (char)b[i];
			// if we find the semicolon
			if (c == ';') {
				numberPart = true;
				num = 0;
			// assemble number
			} else if (numberPart && ( '0' <= c && c <= '9')){
				num *= 10;
				num +=  c - '0';
				
			} else {
				sb.append((char) b[i]);
			}
		}

		output = sb.toString();

		return new Pair<>(output, num);

	}



}
