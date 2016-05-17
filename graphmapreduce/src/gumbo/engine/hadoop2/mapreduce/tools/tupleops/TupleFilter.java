package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;


/**
 * Interface for filtering tuples.
 * 
 * @author Jonny Daenen
 *
 */
public interface TupleFilter {

	boolean check(QuickWrappedTuple qt);

}
