package gumbo.engine.hadoop2.mapreduce.tools;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import gumbo.engine.hadoop2.datatypes.VBytesWritable;

/**
 * Wrapper for tuples in byte representation.
 * 
 * @author Jonny Daenen
 * 
 */
public class QuickWrappedTuple {

	//	private static Pattern p = Pattern.compile("\\(|,|\\)");

	private static final Log LOG = LogFactory.getLog(QuickWrappedTuple.class);


	byte[] data;
	int length;
	int fields = 0;


	LinkedList<Integer> startList;
	LinkedList<Integer> lengthList;
	int maxlength;

	static final byte [] commabytes = ",".getBytes();

	byte [] extra_buffer;
	int extra_length;


	public QuickWrappedTuple() {
		startList = new LinkedList<>();
		lengthList = new LinkedList<>();
		maxlength = 0;
	}

	/**
	 * Sets the capacity of the extra buffer.
	 */
	private void setCapacity(int cap) {
		if (extra_buffer == null || cap > extra_buffer.length) {
			extra_buffer = new byte[cap];
		}
		extra_length = cap;
	}

	/**
	 * Inspects the internal buffer of the Text object
	 * and keeps it for later reference.
	 * Note that the original Text buffer is used 
	 * and changes to the buffer will be reflected.
	 * 
	 * @param t Text object
	 */
	public void initialize(Text t) {
		this.data = t.getBytes();
		this.length = t.getLength();
		initialize();
	}


	/**
	 * Sets the internal buffer to the given one.
	 * Later changes to the buffer will be reflected.
	 * 
	 * @param data2 byte buffer
	 * @param length size of the byte buffer
	 */
	public void initialize(byte[] data2, int length) {
		this.data = data2;
		this.length = length;
		initialize();
	}

	/**
	 * Initialized the lookup lists,
	 * assuming this is a csv tuple. 
	 * The code can be adapted for utf-8 easily, 
	 * by detecting special characters and 
	 * jumping forward accordingly.
	 */
	private void initialize() {
		int start = 0;
		int bytelength = 0;
		startList.clear();
		lengthList.clear();
		fields = 1;

		for (int i = 0; i < length; i++) {
			char c = (char) data[i];
			if (c == ',' ) {
				fields++;
				startList.add(start);
				lengthList.add(bytelength);
				maxlength = Math.max(maxlength, bytelength);

				bytelength = 0;
				start = i+1;
			} else {
				bytelength++;
			}
		}

		if (bytelength > 0) {
			startList.add(start);
			lengthList.add(bytelength);
			maxlength = Math.max(maxlength, bytelength);
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

	/**
	 * Fetches start position of attribute i.
	 * @param i attribute index
	 * @return the start index of the given attribute
	 */
	public int getStart(int i) {
		return startList.get(i);
	}

	/**
	 * Fetches length of attribute i.
	 * @param i attribute index
	 * @return the length in bytes of the given attribute
	 */
	public int getLength(int i) {
		return lengthList.get(i);
	}

	/**
	 * Returns the length in bytes of the internal buffer.
	 * @return size of internal buffer
	 */
	public int getLength() {
		return length;
	}

	/**
	 * Returns the internal buffer.
	 * @return internal buffer
	 */
	public byte[] getData() {
		return data;
	}


	/**
	 * Returns the number of attributes.
	 * @return the number of attributes
	 */
	public int size() {
		return fields;
	}

	/**
	 * Checks wheter two tuples are equal
	 * @param t another tuple
	 * @return true iff both tuples are equal
	 */
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



	/**
	 * Checks whether two fields are equal in size and content.
	 * 
	 * @param i index of field 1
	 * @param j index of field 2
	 * 
	 * @return true iff both fields are equal
	 */
	public boolean equals(int i, int j) {
		int length = lengthList.get(i);
		if (length != lengthList.get(j))
			return false;

		int starti = startList.get(i);
		int startj = startList.get(j);
		int endi = starti + length;

		// OPTIMIZE this can be optimized
		for (; starti < endi; starti++, startj++) {
			if (data[starti] != data[startj])
				return false;
		}


		return true;
	}

	/**
	 * projects data buffer into extra_buffer.
	 * After projection, extra_length has correct buffer size.
	 */
	private void project(List<Integer> fields2) {
		// set extra_buffer limits to full tuple
		setCapacity(fields2.size() * (maxlength + commabytes.length));

		// OPTIMIZE make ByteBuffer a field
		ByteBuffer buffer = ByteBuffer.wrap(extra_buffer);

		// project all fields in order
		for (int i : fields2) {
			buffer.put(data, startList.get(i), lengthList.get(i));
			buffer.put(commabytes, 0, commabytes.length);
		}

		// remove trailing comma
		extra_length = buffer.position() - 1;
	}

	/**
	 * Projects correct fields to output.
	 * @param fields field indices
	 * @param output writable
	 */
	public void project(List<Integer> fields, Text output) {

		project(fields);
		output.set(extra_buffer, 0, extra_length);
	}

	public void project(byte[] keyFields, VBytesWritable output) {
		// set extra_buffer limits to full tuple
		setCapacity(keyFields.length * (maxlength + commabytes.length));

		// OPTIMIZE make ByteBuffer a field
		ByteBuffer buffer = ByteBuffer.wrap(extra_buffer);

		// project all fields in order
		for (byte i : keyFields) {
			if (startList.size() > i) {
				buffer.put(data, startList.get(i), lengthList.get(i));
			}
			buffer.put(commabytes, 0, commabytes.length);
		}

		// remove trailing comma
		extra_length = buffer.position() - 1;
		output.set(extra_buffer, 0, extra_length);

	}





}
