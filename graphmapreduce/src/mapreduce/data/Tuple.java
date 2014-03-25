package mapreduce.data;

import guardedfragment.structure.GFAtomicExpression;

import java.util.HashMap;

public class Tuple {
	
	String name;
	String[] data;
	
	public Tuple(String s){
		String[] t = s.split(new String("\\(|,|\\)"));
		name = t[0];
		data = new String[t.length-1];
		for(int i=0;i< t.length-1;i++){
			data[i]=t[i+1];
		}
	}
	
	public Tuple(String name, String ... data) {
		this.name = name;
		this.data = data;
	}
	
	public String getData(String newname, HashMap<Integer,Integer> f){
		String s = new String();
		
		//System.out.println(f.toString());
		
		for(int i =0; i < f.size(); i++){
			s = s+","+data[f.get(i)];
		}
		return newname + "(" + s.substring(1) + ")";
		
	}
	
	public String generateString() {
		String t = new String();
		
		for(int i =0; i < data.length;i++) {
			t = t+ "," + data[i];
		}
		
		return name+ "(" + t.substring(1)+")";
	}
	
	public boolean belongsTo(GFAtomicExpression R) {
		
		String Rname = R.getName();
		String[] vars = R.getVars();
		
		if (! name.equals(Rname)) {
			return false;
		}
		
		if (vars.length != data.length) {
			return false;
		}
		for(int i = 0; i < data.length ;i++){
			for(int j = i+1; j < data.length;j++) {
				if (vars[i].equals(vars[j]) && !data[i].equals(data[j])) {
					return false;
				}
			}
		}
		return true;
	}
	
	public String get(int i ) {
		return data[i];
	}
	
	public int size() {
		return data.length;
	}
	
	/**
	 * Checks whether a tuple satisfies a given relation schema.
	 * Check is done on number of fields and on relation name.
	 * 
	 * @param s a schema
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

}
