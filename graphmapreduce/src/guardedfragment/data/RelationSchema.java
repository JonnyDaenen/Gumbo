package guardedfragment.data;

public class RelationSchema {
	
	static final String COLPREFIX = "field";

	String name;
	String [] fields;
	
	public RelationSchema(String name, String ... fields) {
		this.name = name;
		this.fields = fields;
	}
	
	public RelationSchema(String name, int numfields) {
		this.name = name;
		
		// generate fields with generic name
		fields = new String [numfields];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = COLPREFIX+1;
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
		if (! (obj instanceof RelationSchema))
			return false;
		
		RelationSchema s2 = (RelationSchema) obj;
		return name.equals(s2.name) && fields.length == s2.fields.length;
	}
	
	@Override
	public int hashCode() {
		return name.hashCode();
	}
	
}
