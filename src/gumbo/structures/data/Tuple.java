package gumbo.structures.data;

import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import gumbo.structures.gfexpressions.GFAtomicExpression;

/**
 * 
 * @author Tony Tan
 * @author Jonny Daenen
 * 
 */
public class Tuple {

	//	private static Pattern p = Pattern.compile("\\(|,|\\)");

	private static final Log LOG = LogFactory.getLog(Tuple.class);

	String name;
	String[] data;
	String representationCache;
	private String csvrepresentationCache;
	LinkedList<String> bufferlist;


	StringBuilder sb;


	/**
	 * Creates a new tuple based on a given String. When the string is
	 * empty/blank, a tuple with empty name/vars is created.
	 * 
	 * OPTIMIZED
	 * 
	 * @param s
	 *            String representation of the tuple, e.g. R(a,2,1)
	 * @throws InterruptedException
	 */
	public Tuple(String s) {

		representationCache = s;

		//		initialize(s.getBytes());
		// // old2
		int fb = StringUtils.indexOf(s, '('); // s.indexOf('(');
		int lb = StringUtils.lastIndexOf(s, ')'); // s.lastIndexOf(')');
		name = s.substring(0, fb);
		String rest = s.substring(fb + 1, lb);
		// data = rest.split(",");

		// name = StringUtils.substringBefore(s,"(");
		// String rest = StringUtils.substringBetween(s,"(",")");
		// data = StringUtils.split(rest, ',');

		// StringTokenizer st = new StringTokenizer(rest, ",");

		int count = 0;
		for (int i = 0; i < rest.length(); i++) {
			if (rest.charAt(i) == ',')
				count++;

		}

		data = new String[count + 1];
		int i = 0;
		int start = 0;
		int end = -1;

		while (i < count) {
			start = end + 1;
			// end = StringUtils.indexOf(rest, ',',start);
			end = rest.indexOf(',', start);
			// data[i] = StringUtils.substring(rest, start,end);
			data[i] = rest.substring(start, end);
			i++;
		}
		// final piece
		start = end + 1;
		data[i] = rest.substring(start);


		//// old
		//
		// String[] t; // = s.split(new String("\\(|,|\\)"));
		// t = p.split(s);
		//
		// if (s.trim().length() == 0 || t.length == 0) {
		// name = "";
		// data = new String[0];
		// }
		//
		// name = t[0];
		// data = new String[t.length - 1];
		// for (int i = 0; i < t.length - 1; i++) {
		// data[i] = t[i + 1];
		// }
	}

	public Tuple(String name, String... data) {
		this.name = name;
		this.data = data;
	}

	public Tuple(byte [] b,int length) {
		initialize(b, length);

	}

	@Deprecated
	public Tuple(Text value) {
		//		this(value.toString());

		byte[] b = value.getBytes();

		initialize(b,value.getLength());
		//		System.out.println(name + " " + data[0] + data[1]);
	}

	public String getData(String newname, HashMap<Integer, Integer> f) {
		String s = new String();

		// System.out.println(f.toString());

		for (int i = 0; i < f.size(); i++) {
			s = s + "," + data[f.get(i)];
		}
		return newname + "(" + s.substring(1) + ")";

	}

	public void clear() {
		if (sb == null) {
			sb = new StringBuilder(200);
		}
		if (bufferlist == null)
			bufferlist = new LinkedList<>();

		sb.setLength(0);

		bufferlist.clear();

		representationCache = null;
		csvrepresentationCache = null;
		data = null;
	}

	public void initialize(String name, byte [] b, int length) {
		clear();
		
		int start = 0;
		int bytelength = 0;
		String s;

		for (int i = 0; i < length; i++) {
			char c = (char) b[i];
			if (c == ',' ) {
				s = new String(b, start, bytelength);
				bufferlist.add(s);
				
				bytelength = 0;
				start = i+1;
			} else {
				bytelength++;
			}
		}

		if (bytelength > 0) {
			s = new String(b, start, bytelength);
			bufferlist.add(s);
		}
		this.name = name;
		data = bufferlist.toArray(new String [0]);
//		csvrepresentationCache = new String(b,0,length);

	}

	public void initialize(byte [] b, int length) {
		clear();


		for (int i = 0; i < length; i++) {
			char c = (char) b[i];
			if (c == '(') {
				this.name = sb.toString();
				sb.setLength(0);
			} else if (c == ',' ) {
				bufferlist.add(sb.toString());
				sb.setLength(0);
			} else if (c == ')') { // do this separately
				//				if (sb.length() > 0) TODO skip empty lines in calling function
				bufferlist.add(sb.toString());
				sb.setLength(0);
				break; // Text can contain extra garbage
			} else if (c == ' ') { // skip spaces
				// FIXME this may not work for constants
				continue;
			} else {
				sb.append(c);
			}

			// System.out.print((char)b[i]);
		}

		//		representationCache = new String(b,0,length).substring(first, last); //.trim();
		//		csvrepresentationCache = representationCache.substring(firstcsv, lastcsv); //.trim();
		data = bufferlist.toArray(new String [0]);
	}

	/**
	 * Returns a String representation of a tuple.
	 * When csv representation is requested, the tuple is not wrapped in "RelationName(...)".
	 * A cache is kept to quickly obtain the representation in future requests.
	 * 
	 * @param csv
	 * @return
	 */
	public String generateString(boolean csv) {

		if (representationCache == null) {
			//			int numChars = 0;
			//			for (String d : data)
			//				numChars += d.length();
			//			numChars += name.length() + data.length + 2; // name,  comma's and brackets

			if (sb == null) {
				sb = new StringBuilder(100);
			}

			sb.setLength(0);

			sb.append(name);
			sb.append('(');


			if (data.length > 0) {
				int i;
				for (i = 0; i < data.length-1; i++) {
					sb.append(data[i]);
					sb.append(',');
				}
				sb.append(data[i]);
			}

			sb.append(')');

			//			sb.insert(0, '(');
			//			sb.insert(0, name);

			representationCache = sb.toString();
		}

		if (csvrepresentationCache == null) {
			csvrepresentationCache = representationCache.substring(representationCache.indexOf('(')+1, representationCache.lastIndexOf(')'));
		}

		if (csv) {
			return csvrepresentationCache;
		} else {
			return representationCache;
		}

	}

	@Deprecated
	public boolean belongsTo(GFAtomicExpression R) {
		return R.matches(this);
	}

	public String get(int i) {
		return data[i];
	}

	/**
	 * 
	 * @return the number of fields
	 */
	public int size() {
		return data.length;
	}

	/**
	 * Checks whether a tuple satisfies a given relation schema. Check is done
	 * on number of fields and on relation name.
	 * 
	 * @param s
	 *            a schema
	 * @return true when the tuple satisfies the schema, false otherwise
	 */
	public boolean satisfiesSchema(RelationSchema s) {
		return data.length == s.getNumFields() && (s.getName().equals(this.name));

	}

	public RelationSchema extractSchema() {
		return new RelationSchema(name, data.length);
	}

	public String getName() {
		return name;
	}

	public String[] getAllData() {
		return data;
	}

	public boolean equals(Tuple t) {
		if (data.length != t.size()) {
			return false;
		}

		if (!name.equals(t.getName())) {
			return false;
		}

		for (int i = 0; i < data.length; i++) {
			if (!data[i].equals(t.get(i))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String toString() {

		return generateString(false);

		//
		// String out = "";
		//
		// // concatenate all fields
		// for (int i = 0; i < data.length; i++) {
		// out += "," + data[i];
		// }
		//
		// // dummy value for substring
		// if (out.length() == 0)
		// out = "-";
		//
		// // add name
		// out = name + "(" + out.substring(1) + ")";
		//
		// representation = out;
		// return representation;
	}

}
