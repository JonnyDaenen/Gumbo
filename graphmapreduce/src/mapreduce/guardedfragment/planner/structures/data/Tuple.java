package mapreduce.guardedfragment.planner.structures.data;

import java.util.HashMap;
import java.util.regex.Pattern;

import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;

/**
 * 
 * @author Tony Tan
 * @author Jonny Daenen
 * 
 */
public class Tuple {

	private static Pattern p = Pattern.compile("\\(|,|\\)");

	String name;
	String[] data;
	String representation;

	/**
	 * Creates a new tuple based on a given String. When the string is
	 * empty/blank, a tuple with empty name/vars is created.
	 * 
	 * @param s
	 *            String representation of the tuple, e.g. R(a,2,1)
	 */
	public Tuple(String s) {

		representation = s;

		int fb = s.indexOf('(');
		int lb = s.lastIndexOf(')');

		name = s.substring(0, fb);
		String rest = s.substring(fb + 1, lb);
		data = rest.split(",");

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

	public String getData(String newname, HashMap<Integer, Integer> f) {
		String s = new String();

		// System.out.println(f.toString());

		for (int i = 0; i < f.size(); i++) {
			s = s + "," + data[f.get(i)];
		}
		return newname + "(" + s.substring(1) + ")";

	}

	public String generateString() {

		if (representation == null) {
			StringBuilder sb = new StringBuilder(data.length * 5);

			for (int i = 0; i < data.length; i++) {
				sb.append(data[i]);
				sb.append(',');
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append(')');
			sb.insert(0, '(');
			sb.insert(0, name);

			representation = sb.toString();
		}

		return representation;

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
		if (!name.equals(t.getName())) {
			return false;
		}
		if (data.length != t.size()) {
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

		if (representation != null) {
			return representation;
		}

		representation = generateString();
		return representation;

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
