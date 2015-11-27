package gumbo.engine.hadoop2.datastructures;

import gumbo.structures.gfexpressions.GFAtomicExpression;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * Wrapper for Text objects.
 * 
 * @author Jonny Daenen
 * 
 */
public class QuickWrappedTuple {

	//	private static Pattern p = Pattern.compile("\\(|,|\\)");

	private static final Log LOG = LogFactory.getLog(QuickWrappedTuple.class);


	byte[] data;
	int fields = 0;

	LinkedList<Integer> startList;
	LinkedList<Integer> lengthList;


	public QuickWrappedTuple() {
		startList = new LinkedList<>();
		lengthList = new LinkedList<>();
	}

	public QuickWrappedTuple(Text t) {

		startList = new LinkedList<>();
		lengthList = new LinkedList<>();
		initialize(t);
	}


	public void initialize(Text t) {

		this.data = t.getBytes();

		int start = 0;
		int bytelength = 0;
		startList.clear();
		lengthList.clear();
		fields = 1;

		for (int i = 0; i < data.length; i++) {
			char c = (char) data[i];
			if (c == ',' ) {
				fields++;
				startList.add(start);
				lengthList.add(bytelength);

				bytelength = 0;
				start = i+1;
			} else {
				bytelength++;
			}
		}

		if (bytelength > 0) {
			startList.add(start);
			lengthList.add(bytelength);
		}

	}


	public void initialize_slow(Text t) {


		this.data = t.getBytes();

		startList.clear();
		lengthList.clear();

		int prevpos = 0;
		int pos;
		fields = 1;
		while ((pos = t.find(",", prevpos)) != -1 ) {
			startList.add(prevpos);
			lengthList.add(pos - prevpos);
			prevpos = pos + 1;
			fields++;
		}
		startList.add(prevpos);
		lengthList.add(pos - prevpos);


	}

	public int getStart(int i) {
		return startList.get(i);
	}

	public int getLength(int i) {
		return lengthList.get(i);
	}

	public byte[] getData() {
		return data;
	}


	/**
	 * 
	 * @return the number of fields
	 */
	public int size() {
		return fields;
	}




	public boolean equals(QuickWrappedTuple t) {
		if (data.length != t.size()) {
			return false;
		}

		for (int i = 0; i < data.length; i++) {
			if (data[i] != t.data[i]) {
				return false;
			}
		}
		return true;
	}

}
