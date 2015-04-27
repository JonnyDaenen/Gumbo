package gumbo.structures.data;

import gumbo.structures.gfexpressions.GFAtomicExpression;

import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author Tony Tan
 * @author Jonny Daenen
 * 
 */
public class Tuple {

//	private static Pattern p = Pattern.compile("\\(|,|\\)");

	String name;
	String[] data;
	String representationCache;

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

	public Tuple(byte [] b) {
		initialize(b);
		
	}
	/**
	 * @param value
	 */
	public Tuple(Text value) {
//		this(value.toString());

		byte[] b = value.getBytes();

		initialize(b);
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
	
	private void initialize(byte [] b) {
		StringBuilder sb = new StringBuilder(b.length);
		LinkedList<String> list = new LinkedList<>();
		for (int i = 0; i < b.length; i++) {
			char c = (char) b[i];
			if (c == '(') {
				this.name = sb.toString();
				sb.setLength(0);
			} else if (c == ',' ) {
				list.add(sb.toString());
				sb.setLength(0);
			} else if (c == ')') { // do this separately
				list.add(sb.toString());
				sb.setLength(0);
				break; // Text can contain extra garbage
			} else if (c == ' ') { // skip spaces
				continue;
			} else {
				sb.append(c);
			}
			
			// System.out.print((char)b[i]);
		}
		data = list.toArray(new String [0]);
	}

	public String generateString() {

		if (representationCache == null) {
			StringBuilder sb = new StringBuilder(data.length * 10);


			sb.append(name);
			sb.append('(');
			
			for (int i = 0; i < data.length; i++) {
				sb.append(data[i]);
				sb.append(',');
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append(')');
//			sb.insert(0, '(');
//			sb.insert(0, name);

			representationCache = sb.toString();
		}

		return representationCache;

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

		return generateString();

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
