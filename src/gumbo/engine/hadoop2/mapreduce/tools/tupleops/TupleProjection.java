package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;

/**
 * Interface for tuple projection operation.
 * 
 * @author Jonny Daenen
 *
 */
public interface TupleProjection {

	public boolean load(QuickWrappedTuple qt, long offset, VBytesWritable bw, GumboMessageWritable gw);

	public boolean canMerge(TupleProjection pi2);

	public TupleProjection merge(TupleProjection pi2);

}
