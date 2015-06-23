/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.reducers;

import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Red1Algorithm;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Red1MessageFactory;
import gumbo.engine.hadoop.mrcomponents.tools.ParameterPasser;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Uses atom data generated by the corresponding mapper.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFReducer1Optimized extends Reducer<Text, Text, Text, Text> {

	private final static String FILENAME = "round1";


	private static final Log LOG = LogFactory.getLog(GFReducer1Optimized.class);


	private Red1Algorithm algo;


	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// load context
		super.setup(context);

		String s = String.format("Reducer"+this.getClass().getSimpleName()+"-%05d-%d",
				context.getTaskAttemptID().getTaskID().getId(),
				context.getTaskAttemptID().getId());
		LOG.info(s);



		// load parameters
		try {
			Configuration conf = context.getConfiguration();

			ParameterPasser pp = new ParameterPasser(conf);
			ExpressionSetOperations eso = pp.loadESO();
			HadoopExecutorSettings settings = pp.loadSettings();

			Red1MessageFactory msgFactory = new Red1MessageFactory(context, settings, eso, FILENAME);
			algo = new Red1Algorithm(eso,settings,msgFactory);

		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException("Reducer initialisation error: " + e.getMessage());
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		try {
			algo.cleanup();
		} catch(Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		try {
			
			algo.initialize(key.toString());

			// WARNING Text object will be reused by Hadoop!
			for (Text t : values) {

				// parse input
				Pair<String, String> split = split(t);

				// feed it to algo
				if(!algo.processTuple(split))
					break;

			}

			// indicate end of tuples
			// and finish calculation
			algo.finish();

		} catch(Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
		
		//		boolean keyFound = false;
		//
		//		// WARNING Text object will be reused by Hadoop!
		//		for (Text t : values) {
		//
		//			//			if (print)
		//			//				LOG.error("Red1: " + key + " " + t);
		//
		//			// parse input
		//			Pair<String, String> split = split(t);
		//
		//
		//			// is this not the key (key is only thing that can appear without atom reference)
		//			// it does not matter whether it's sent as S(1) or with a constant symbol such as '#'
		//			if (split.snd.length() > 0) {
		//
		//				msgFactory.loadValue(split.fst, split.snd);
		//
		//				// if the key has already been found, we can output
		//				if (keyFound) {
		//					msgFactory.sendReply();
		//				}
		//				// if optimization is on, we know that if the key is not there, we can skip the rest
		//				else if (finiteMemOptOn) {
		//					ABORTS.increment(1);
		//					break;
		//				} 
		//				// otherwise, we buffer the data
		//				else {
		//					buffer.add(split);
		//					BUFFERED.increment(1);
		//				}
		//
		//			} // if this is the key, we mark it
		//			else if (!keyFound) {
		//				keyFound = true;
		//			}
		//		}
		//
		//		// output the remaining data
		//		if (keyFound) {
		//			for (Pair<String, String> p : buffer) {
		//				msgFactory.loadValue(p.fst, p.snd);
		//				msgFactory.sendReply();
		//			}
		//		}
		//
		//		// clear the buffer for next round
		//		// this is done in the end in case hadoop invokes GC
		//		// (not sure whether hadoop does this in between calls)
		//		buffer.clear();

	}

	/**
	 * Splits String into 2 parts. String is supposed to be separated with ';'.
	 * When no ';' is present, the numeric value is -1. 
	 * @param t
	 */
	protected Pair<String, String> split(Text t) {


		int pos = t.find(";");

		byte [] bytes = t.getBytes();
		int length = t.getLength(); // to fix internal byte buffer length mismatch

		String address = "";
		String reply = "";

		if (pos != -1) {
			address = new String(bytes, 0, pos);
			reply = new String(bytes, pos+1,length-pos-1); // 1-offset is to skip ';'
		} else {
			address = new String(bytes,0,length);
			reply = "";
		}

		return new Pair<>(address, reply);

	}



}
