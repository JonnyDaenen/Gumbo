/**
 * Created on: 07 Apr 2015
 */
package gumbo.engine.hadoop.mrcomponents.round1.comparators;

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
}
