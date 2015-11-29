package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import org.apache.hadoop.io.BytesWritable;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;

public interface TupleProjection {

	public void project(QuickWrappedTuple qt, BytesWritable bw, GumboMessageWritable gw);

	public boolean matches(QuickWrappedTuple qt);

	public String getFilename();

}
