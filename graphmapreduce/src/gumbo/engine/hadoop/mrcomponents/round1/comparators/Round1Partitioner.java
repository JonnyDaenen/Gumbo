/**
 * Created on: 07 Apr 2015
 */
package gumbo.engine.hadoop.mrcomponents.round1.comparators;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Jonny Daenen
 *
 */
public class Round1Partitioner extends Partitioner<Text, Text> {

	Charset charset = StandardCharsets.UTF_8;
	CharsetDecoder decoder = charset.newDecoder();
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {

		// use buffer wrapping to avoid allocation
		try {
			ByteBuffer bb1 = ByteBuffer.wrap(((Text)key).getBytes(),0,((Text)key).getLength());
			CharBuffer charbuf1 = decoder.decode(bb1);

			int val =  getHashCode(charbuf1);
			return Math.abs(val) % numPartitions;
		} catch (Exception e) {

		}

		// fall-back mechanism (old way)
		
		KeyPairWrapper wrap = new KeyPairWrapper(key.toString());
		int hash = wrap.first.hashCode();
		int partition = Math.abs(hash) % numPartitions;
		return partition;
	}

	// TEST create unit test with multiple reducers!
	private int getHashCode(CharBuffer cb) {
		int h = 0;
		int length = cb.length();
		
		if (cb.charAt(cb.length() - 1 ) == '#')
			length--;
		
		for (int i = 0; i < length; i++) {
			h = 31 * h + cb.get(i);
		}
		return h;
	}
	

}
