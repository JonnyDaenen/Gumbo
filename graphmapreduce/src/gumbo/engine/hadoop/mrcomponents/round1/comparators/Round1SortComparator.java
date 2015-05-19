/**
 * Created on: 07 Apr 2015
 */
package gumbo.engine.hadoop.mrcomponents.round1.comparators;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Jonny Daenen
 *
 */
public class Round1SortComparator extends WritableComparator {

	protected Round1SortComparator() {
		super(Text.class,null,true);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		Charset charset = StandardCharsets.UTF_8;
		CharsetDecoder decoder = charset.newDecoder();

		// No allocation performed, just wraps the StringBuilder.
		//		CharBuffer buffer = CharBuffer.wrap(stringBuilder);
		try {
			ByteBuffer bb1 = ByteBuffer.wrap(((Text)a).getBytes(),0,((Text)a).getLength());
			CharBuffer charbuf1 = decoder.decode(bb1);
			
			ByteBuffer bb2 = ByteBuffer.wrap(((Text)b).getBytes(),0,((Text)b).getLength());
			CharBuffer charbuf2 = decoder.decode(bb2);

			
			int val =  compare(charbuf1, charbuf2);
			return val;
		} catch (Exception e) {

		}
		// backup mechanism
		
		// convert
		KeyPairWrapper first = new KeyPairWrapper(((Text)a).toString());
		KeyPairWrapper second = new KeyPairWrapper(((Text)b).toString());

		// sort on first field
		int diff = first.first.compareTo(second.first);
		// sort on second field
		if (diff == 0) {
			diff = first.second.compareTo(second.second);
		} 

		//		System.out.println(a + " - " + b + diff);
		//		System.out.println(first + " - " + second + diff);




		//		if ((((Text)a).toString()+((Text)a).toString()).contains("(0")) {
		//			System.out.println("o one:" + first);
		//			System.out.println("o two:" + second);
		//			System.out.println(diff);
		//		}
		return diff;
	}



	//	@Override
	//	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	//
	//		System.out.println("one:" + b1[s1]);
	//		System.out.println("two:" + b2[s2]);
	//
	//		return super.compare(b1, s1, l1, b2, s2, l2);
	//	}


	private int compare(CharBuffer cb1, CharBuffer cb2) {

		char lastChar1 = cb1.get(cb1.length()-1);
		char lastChar2 = cb2.get(cb2.length()-1);
		
		

		int len1 = cb1.length() - (lastChar1=='#'?1:0);
		int len2 = cb2.length() - (lastChar2=='#'?1:0);
		int lim = Math.min(len1, len2);

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
			return lastChar1=='#'?1:0 - lastChar2=='#'?1:0;
		return result;
	}
}
