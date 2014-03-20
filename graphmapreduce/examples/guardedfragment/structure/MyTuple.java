package guardedfragment.structure;

import java.util.HashMap;

public class MyTuple {
	
	String name;
	String[] data;
	
	public MyTuple(String s){
		String[] t = s.split(new String("\\(|,|\\)"));
		name = t[0];
		data = new String[t.length-1];
		for(int i=0;i< t.length-1;i++){
			data[i]=t[i+1];
		}
	}
	
	public String[] getData(HashMap<Integer,Integer> f){
		String[] s = new String[f.size()];
		
		for(int i =0; i < f.size(); i++){
			s[i] = data[f.get(i)];
		}
		
		return s;
		
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
	

}
