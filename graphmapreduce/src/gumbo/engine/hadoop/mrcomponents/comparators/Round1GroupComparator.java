/**
 * Created on: 07 Apr 2015
 */
package gumbo.engine.hadoop.mrcomponents.comparators;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Jonny Daenen
 *
 */
public class Round1GroupComparator extends WritableComparator {

	
	 protected Round1GroupComparator() {
	        super(Text.class,null,true);
	    }
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		// convert
				KeyPairWrapper first = new KeyPairWrapper(((Text)a).toString());
				KeyPairWrapper second = new KeyPairWrapper(((Text)b).toString());
				
				int diff = first.first.compareTo(second.first);				
				
//				if ((((Text)a).toString()+((Text)a).toString()).contains("(0")) {
//					System.out.println("Group one:" + first);
//					System.out.println("Group two:" + second);
//					System.out.println(diff);
//				}
				return diff;
	}


//	@Override
//	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//
//		System.out.println("G one:" + b1[s1]);
//		System.out.println("G two:" + b2[s2]);
//		
//		return super.compare(b1, s1, l1, b2, s2, l2);
//	}


}
