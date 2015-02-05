package mapreduce.guardedfragment.structure;

import gumbo.compiler.structures.data.RelationSchema;
import gumbo.compiler.structures.data.Tuple;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;

public class ProjectionExample {
	
	public static void main(String[] args) {
		
		// Expresions
		GFAtomicExpression e1 = new GFAtomicExpression("R","x","y");
		GFAtomicExpression e2 = new GFAtomicExpression("S","y","x");
		
		System.out.println(e1);
		System.out.println(e2);
		
		
		// Schemas
		RelationSchema s1 = e1.extractRelationSchema();
		RelationSchema s2 = e2.extractRelationSchema();
		
		System.out.println(s1);
		System.out.println(s2);
		
		
		// Tuples
		Tuple t1 = new Tuple("R","1","2");
		Tuple t2 = new Tuple("S","2","1");
		
		System.out.println(t1);
		System.out.println(t2);
		
		
		// Projection
		//Projection pi = ProjectionFactory.createProjection(e1, e2);
		//System.out.println(pi);
		
		
	}

}
