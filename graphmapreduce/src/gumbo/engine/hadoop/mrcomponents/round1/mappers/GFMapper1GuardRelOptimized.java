/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.mappers;

import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardAlgorithm;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper1GuardRelOptimized extends GFMapper1Identity {



	private static final Log LOG = LogFactory.getLog(GFMapper1GuardRelOptimized.class);

	protected Map1GuardMessageFactory msgFactory;
	private Map1GuardAlgorithm algo;



	/**
	 * @see gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		msgFactory = new Map1GuardMessageFactory(context,settings,eso);		
		algo = new Map1GuardAlgorithm(eso, msgFactory);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Tuple t = new Tuple(value.getBytes(), value.getLength());
			algo.run(t, key.get());
	}




}
