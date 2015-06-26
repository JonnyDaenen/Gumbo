package gumbo.engine.hadoop.converter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.AlgorithmInterruptedException;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardAlgorithm;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedAlgorithm;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.MapAlgorithm;
import gumbo.engine.hadoop.reporter.FakeMapper;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.utils.estimation.RandomTupleEstimator;
import gumbo.utils.estimation.Sampler;

/**
 * Tuple and byte size estimator for map 1 output.
 * @author Jonny Daenen
 *
 */
public class IntermediateEstimator {

	RelationFileMapping mapping;
	AbstractExecutorSettings settings;

	public long estNumTuples(RelationSchema rs, CalculationGroup group) {


		long tuples = mapping.visitAllPaths(rs, new RandomTupleEstimator());

		int replication = 1;

		// if it is a guard
		if (group.isGuard(rs)) {

			if (!settings.getBooleanProperty(settings.requestGroupingOptimizationOn)) {
				// if grouping is off, we adjust the replication level
				// by the number of the keys (-1, because we started out with 1)
				replication += group.getKeys(rs) - 1;

			}

			// TODO
			// if it is also guarded
			// adjust replication

		} 

		return tuples * replication;

	}

	public long estNumBytes(RelationSchema rs, CalculationGroup group) {

		long tuples = mapping.visitAllPaths(rs, new RandomTupleEstimator());
		long byteSize = mapping.getRelationSize(rs);


		// fetch sample set
		long samplesize = 4*1024; // 4kb
		int numSamplesPerPath = 10;



		try {
			// create new expressions


			// create algorithm
			MapAlgorithm algo;

			Collection<GFExistentialExpression> expressionSet = null; // FIXME create eso
			FakeMapper fm = new FakeMapper();
			ExpressionSetOperations eso;
			eso = new ExpressionSetOperations(expressionSet, mapping);


			if (group.isGuard(rs)) {
				algo = new Map1GuardAlgorithm(eso, new Map1GuardMessageFactory(fm.context, settings, eso));
			} else {
				algo = new Map1GuardedAlgorithm(eso, new Map1GuardedMessageFactory(fm.context, settings, eso));

			}



			for (Path path: mapping.getPaths(rs)) {
				byte [][] rawbytes = Sampler.getRandomBlocks(path, numSamplesPerPath, samplesize);

				for (int i = 0; i < numSamplesPerPath; i++) {
					// read text lines
					Text t = new Text();
					LineReader l = new LineReader(new ByteArrayInputStream(rawbytes[0]));
					
					// skip first line, as it may be incomplete
					long bytesread = l.readLine(t); 

					long dummyoffset = 0;
					while (true) {
						bytesread = l.readLine(t); 
						if (bytesread <= 0)
							break;

						// if csv, wrap
						InputFormat format = mapping.getInputFormat(rs);

						String s = t.toString();
						if (format == InputFormat.CSV) {
							s = rs.getName() + "(" + s + ")";
						}

						Tuple tuple = new Tuple(s);

						algo.run(tuple, dummyoffset);
						dummyoffset += bytesread;
					}
				}
			}

			// get results from fake context
			// TODO

			// measure output

			// extrapolate measurements
		} catch (GFOperationInitException | AlgorithmInterruptedException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}


	public long getTotalBytes(CalculationGroup group) {
		long total = 0;
		for ( GFAtomicExpression exp : group.getGuardDistinctList()) {
			total += estNumBytes(exp.getRelationSchema(),group);
		}

		for ( GFAtomicExpression exp : group.getGuardedDistinctList()) {
			total += estNumBytes(exp.getRelationSchema(),group);
		}
		return total;
	}

}
