package gumbo.engine.hadoop2.mapreduce.evaluate;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable.GumboMessageType;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleFilter;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleOpFactory;
import gumbo.structures.gfexpressions.GFAtomicExpression;

public class IdentityMapper extends Mapper<BytesWritable, GumboMessageWritable, BytesWritable, GumboMessageWritable> {



	@Override
	protected void map(BytesWritable key, GumboMessageWritable value,
			Mapper<BytesWritable, GumboMessageWritable, BytesWritable, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {

		// write to output
		context.write(key, value);

	}
}
