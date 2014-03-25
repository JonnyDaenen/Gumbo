package guardedfragment.structure;

import mapreduce.data.Projection;

public class ProjectionFactory {
	
	
	public static Projection createProjection(GFAtomicExpression e1, GFAtomicExpression e2) {
		
		Projection pi = new Projection(e1.extractRelationSchema(), e2.extractRelationSchema());
		
		String[] vars1 = e1.getVars();
		String[] vars2 = e1.getVars();
		
		// for each var in the source 
		for (int i = 0; i < vars1.length; i++) {
			String varname1 = vars1[i];
			
			// compare to each var in the target
			for (int j = 0; j < vars2.length; j++) {
				String varname2 = vars1[i];
				
				if(varname1.equals(varname2))
					pi.addMapping(i, j);
			}
			
		}
		
		return pi;
	}

}
