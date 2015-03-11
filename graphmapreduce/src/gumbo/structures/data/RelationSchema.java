package gumbo.structures.data;

import org.apache.commons.lang3.StringUtils;

/**
 * FIXME remove arity as an identifier.
 * @author Jonny Daenen
 *
 */
public class RelationSchema {

	static final String COLPREFIX = "x";

	String name;
	String[] fields;
	
	public RelationSchema(String s) throws RelationSchemaException {
		int fb = StringUtils.indexOf(s, '('); // s.indexOf('(');
		int lb = StringUtils.lastIndexOf(s, ')'); // s.lastIndexOf(')');
		
		
		
		if (s.substring(fb+1, lb).contains("(") || s.substring(fb+1, lb).contains(")")
				|| lb != s.length()-1) {
			throw new RelationSchemaException("Wrong name for a relation schema: "+ s);
			
		}

		name = s.substring(0, fb);
		String rest = s.substring(fb + 1, lb);

		int count = 0;
		for (int i = 0; i < rest.length(); i++) {
			if (rest.charAt(i) == ',')
				count++;

		}

		fields = new String[count + 1];
		int i = 0;
		int start = 0;
		int end = -1;

		while (i < count) {
			start = end + 1;
			// end = StringUtils.indexOf(rest, ',',start);
			end = rest.indexOf(',', start);
			// data[i] = StringUtils.substring(rest, start,end);
			fields[i] = rest.substring(start, end);
			i++;
		}
		// final piece
		start = end + 1;
		fields[i] = rest.substring(start);
	}
	
	
	public RelationSchema(String name, String... fields) {
		this.name = name;
		this.fields = fields;
	}

	public RelationSchema(String name, int numfields) {
		this.name = name;

		// generate fields with generic name
		fields = new String[numfields];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = COLPREFIX + i;
		}

	}

	public String getName() {
		return name;
	}

	public String[] getFields() {
		return fields;
	}

	public int getNumFields() {
		return fields.length;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof RelationSchema))
			return false;

		RelationSchema s2 = (RelationSchema) obj;
		return name.equals(s2.name) && fields.length == s2.fields.length;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public String toString() {

		String out = "";

		// concatenate all fields
		for (int i = 0; i < fields.length; i++) {
			out += "," + fields[i];
		}

		// dummy value for substring
		if (out.length() == 0)
			out = "-";

		// add name
		out = name + "(" + out.substring(1) + ")";

		return out;
	}

	/**
	 * Generates a short description for the relation schema. This description
	 * is based on the relation name and the number of fields.
	 * 
	 * @return a short description of the relation schema
	 */
	public String getShortDescription() {
		return getName() + getNumFields();
	}
	


}
