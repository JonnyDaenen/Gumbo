/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.reducers;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.planner.structures.operations.GFReducer;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Uses atom data generated by the corresponding mapper.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFReducer1AtomBased extends GFReducer implements Serializable {

	private static final long serialVersionUID = 1L;
	private final static String FILENAME = "tmp_round1_red.txt";

	private static final Log LOG = LogFactory.getLog(GFReducer1AtomBased.class);

	/**
	 * @throws GFOperationInitException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFReducer#reduce(java.lang.String,
	 *      java.lang.Iterable)
	 */
	@Override
	// / OPTIMIZE iterable string?
	public HashSet<Pair<Text, String>> reduce(Text key, Iterable<? extends Object> values)
			throws GFOperationInitException {

		HashSet<Pair<Text, String>> result = new HashSet<Pair<Text, String>>();
		Set<String> buffer = new HashSet<>();
		
		
		boolean keyFound = false;
		for (Object v : values) {
			
			// WARNING Text object will be reused by Hadoop!
			Text t = (Text) v;
			
			// is this a guard
			if ( t.find(";") >= 0) {
				
				// if the key has already been found, we can just output 
				// TODO this is mainly for when we turn this thing into an iterator
				if (keyFound) {
					result.add(new Pair<>(new Text(t),FILENAME));
				}
				// else we collect the data
				else {
					// create new object because Text object will be reduce by Hadoop
					buffer.add(t.toString());
				}
				
				
			// if this is the key, we mark it
			} else if (!keyFound) {
				keyFound = true;
			}
		}
		
		// output the remaining data
		Text out = new Text();
		if (keyFound) {
			for (String p : buffer) {
				out.set(p);
				result.add(new Pair<>(out,FILENAME));
			}
		}

	
		
		return result;
	}

	
	/**
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFReducer#reduce(org.apache.hadoop.io.Text, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, MultipleOutputs<Text, Text> mos) throws IOException, InterruptedException {
		
		Set<Pair<String,String>> buffer = new HashSet<>();
		Text out1 = new Text();
		Text out2 = new Text();
		
//		LOG.warn(key + ": ");
		
		boolean keyFound = false;
		for (Object v : values) {
			
			// WARNING Text object will be reused by Hadoop!
			Text t = (Text) v;
//			LOG.warn("\t" + t);
			String [] split = split(t);
			// is this a guard
			if (split != null) {
				
				// if the key has already been found, we can just output 
				if (keyFound) {
					out1.set(split[0]);
					out2.set(split[1]);
					mos.write(out1, out2, FILENAME);
				}
				// else we collect the data
				else {
					// create new object because Text object will be reduce by Hadoop
					buffer.add(new Pair<>(split[0],split[1]));
				}
				
				
			// if this is the key, we mark it
			} else if (!keyFound) {
				keyFound = true;
			}
		}
		
		// output the remaining data
		Text out = new Text();
		if (keyFound) {
			for (Pair<String,String> p : buffer) {
				out1.set(p.fst);
				out2.set(p.snd);
				mos.write(out1, out2, FILENAME);
			}
		}

		
		
	}


	/**
	 * @param t
	 * @param c
	 */
	private String[] split(Text t) {
		int length = t.getLength();
		StringBuilder sb = new StringBuilder(length);
		String [] output = null;
		
		byte [] b = t.getBytes();
		for(int i = 0; i < length; i++) { // FUTURE for unicode this doesn't work i guess..
			
			// if we find the semicolon
			if((char)b[i] == ';') {
				output = new String[2];
				output[0] = sb.toString();
				sb.setLength(0);
			} else {
				sb.append((char)b[i]);
			}
		}
		if (output != null)
			output[1] = sb.toString();
		
		return output;
		
	}


}
