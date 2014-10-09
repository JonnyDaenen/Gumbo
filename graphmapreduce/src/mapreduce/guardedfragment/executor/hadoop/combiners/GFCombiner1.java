/**
 * Created: 09 Oct 2014
 */
package mapreduce.guardedfragment.executor.hadoop.combiners;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author jonny
 *
 */
public class GFCombiner1 extends Reducer<Text,Text,Text,Text>{

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context c)
			throws IOException, InterruptedException {
		
		// TODO
	}

}
