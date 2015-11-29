package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.Set;

import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class TupleOpFactory {

	public static TupleProjection[] createMap1Projections(String relation, long fileid,
			Set<GFExistentialExpression> queries) {
		// TODO implement
		return null;
	}

	public static TupleEvaluator[] createRed2Projections(Set<GFExistentialExpression> queries) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static TupleFilter createMap2Filter(Set<GFAtomicExpression> atoms, String relation) {
		// TODO Auto-generated method stub
		return null;
	}

}
