/**
 * Created on: 07 Apr 2015
 */
package gumbo.engine.hadoop.mrcomponents.comparators;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Jonny Daenen
 *
 */
public class Round1Partitioner extends Partitioner<Text, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		KeyPairWrapper wrap = new KeyPairWrapper(key.toString());
		int hash = wrap.first.hashCode();
		int partition = Math.abs(hash) % numPartitions;
		return partition;
	}

}
