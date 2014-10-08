package mapreduce.guardedfragment.planner.structures.data;

public class RelationSchema {

	static final String COLPREFIX = "x";

	String name;
	String[] fields;

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
