package gumbo.structures.data;

import gumbo.structures.gfexpressions.GFAtomicExpression;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author Jonny Daenen
 * 
 */
public class QuickTuple {

	//	private static Pattern p = Pattern.compile("\\(|,|\\)");

	private static final Log LOG = LogFactory.getLog(QuickTuple.class);

	String nameS;
	byte[] name;
	byte[] data;
	Integer[] startIndexes;
	Integer [] lengths;

	LinkedList<Integer> startList;
	LinkedList<Integer> lengthList;


	
	public QuickTuple(byte [] name, byte[] data) {
		
		startList = new LinkedList<>();
		lengthList = new LinkedList<>();
		initialize(name, data);
	}


	public void initialize(byte [] name, byte [] data) {

		this.name = name;
		this.data = data;

		int start = 0;
		int bytelength = 0;
		startList.clear();
		lengthList.clear();

		for (int i = 0; i < data.length; i++) {
			char c = (char) data[i];
			if (c == ',' ) {
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
		startIndexes = startList.toArray(new Integer [0]);
		lengths = lengthList.toArray(new Integer [0]);
		//		csvrepresentationCache = new String(b,0,length);

	}

	public int getStart(int i) {
		return startIndexes[i];
	}

	public int getLength(int i) {
		return lengths[i];
	}
	
	public byte[] getData() {
		return data;
	}


	/**
	 * 
	 * @return the number of fields
	 */
	public int size() {
		return startIndexes.length;
	}


	public String getName() {
		return nameS;
	}

	public boolean equals(QuickTuple t) {
		if (data.length != t.size()) {
			return false;
		}

		if (!name.equals(t.getName())) {
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
