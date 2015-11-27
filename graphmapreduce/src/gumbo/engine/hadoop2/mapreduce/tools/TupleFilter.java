package gumbo.engine.hadoop2.mapreduce.tools;

public interface TupleFilter {

	boolean check(QuickWrappedTuple qt);

}
