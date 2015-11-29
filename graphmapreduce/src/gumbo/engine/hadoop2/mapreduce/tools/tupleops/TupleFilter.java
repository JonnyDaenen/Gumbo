package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;

public interface TupleFilter {

	boolean check(QuickWrappedTuple qt);

}
