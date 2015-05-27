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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Jonny Daenen
 *
 */
public class Round1SortComparator extends WritableComparator {

	Charset charset = StandardCharsets.UTF_8;
	CharsetDecoder decoder = charset.newDecoder();

	protected Round1SortComparator() {
		super(Text.class,null,true);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {



		// use buffer wrapping to avoid allocation
		try {
			ByteBuffer bb1 = ByteBuffer.wrap(((Text)a).getBytes(),0,((Text)a).getLength());
			CharBuffer charbuf1 = decoder.decode(bb1);

			ByteBuffer bb2 = ByteBuffer.wrap(((Text)b).getBytes(),0,((Text)b).getLength());
			CharBuffer charbuf2 = decoder.decode(bb2);


			int val =  compare(charbuf1, charbuf2);
			return val;
		} catch (Exception e) {

		}


		// fall-back mechanism (old way)

		// convert
		KeyPairWrapper first = new KeyPairWrapper(((Text)a).toString());
		KeyPairWrapper second = new KeyPairWrapper(((Text)b).toString());

		// sort on first field
		int diff = first.first.compareTo(second.first);
		// sort on second field
		if (diff == 0) {
			diff = first.second.compareTo(second.second);
		} 

		return diff;
	}



	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			ByteBuffer bb1 = ByteBuffer.wrap(b1,s1+1,l1-1); // first byte is length, so we skip it
			CharBuffer charbuf1 = decoder.decode(bb1);

			ByteBuffer bb2 = ByteBuffer.wrap(b2,s2+1,l2-1);
			CharBuffer charbuf2 = decoder.decode(bb2);

			int val =  compareBuffers(charbuf1, charbuf2);
//			System.out.println(charbuf1 + " " + charbuf2 + " " + val + " " + charbuf1.length() + " " + l1);
			return val;
		} catch (Exception e) {

		}

		// fallback 
		return super.compare(b1, s1, l1, b2, s2, l2);
	}


	private int compareBuffers(CharBuffer cb1, CharBuffer cb2) {

		int len1 = cb1.length();
		int len2 = cb2.length();

		char lastChar1 = cb1.get(len1-1);
		char lastChar2 = cb2.get(len2-1);


		if (lastChar1 == '#')
			len1--;
		if (lastChar2 == '#')
			len2--;
		int lim = Math.min(len1, len2);
		
//		System.out.println(lastChar1 + " " + lastChar2 + " " + len1 + " " + len2);

		int k = 0;
		while (k < lim) {
			char c1 = cb1.get(k);
			char c2 = cb2.get(k);
			if (c1 != c2) {
				return c1 - c2;
			}
			k++;
		}
		int result = len1 - len2;

		if (result == 0)
			result = (lastChar2=='#'?1:0) - (lastChar1=='#'?1:0); // !! order is important!

		return result;
	}
}
