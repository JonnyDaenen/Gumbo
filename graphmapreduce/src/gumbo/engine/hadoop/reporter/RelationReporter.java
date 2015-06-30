package gumbo.engine.hadoop.reporter;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.AlgorithmInterruptedException;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardAlgorithm;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedAlgorithm;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.MapAlgorithm;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.utils.estimation.Sampler;
import gumbo.utils.estimation.SamplingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class RelationReporter {

	RelationFileMapping mapping;
	AbstractExecutorSettings settings;

	private static final int SAMPLES_PER_FILE = 10;
	private static final int SAMPLE_SIZE = 40*  1024;

	public RelationReporter(RelationFileMapping mapping, AbstractExecutorSettings settings) {
		this.mapping = mapping;
		this.settings = settings;
	}

	public Map<RelationSchema, RelationReport> generateReports(Collection<GFExistentialExpression> group) throws GFOperationInitException {

		HashMap<RelationSchema, RelationReport> reports = new HashMap<>();


		ExpressionSetOperations eso = new ExpressionSetOperations(group, mapping);

		// guard
		for (GFAtomicExpression guard : eso.getGuardsAll()){
			RelationSchema relation = guard.getRelationSchema();
			reports.put(relation, generateReport(relation, group, eso, true));
		}

		//guarded
		for (GFAtomicExpression guarded : eso.getGuardedsAll()){
			RelationSchema relation = guarded.getRelationSchema();
			reports.put(relation, generateReport(relation, group, eso, false));
		}

		return reports;
	}

	private RelationReport generateReport(RelationSchema relation,
			Collection<GFExistentialExpression> group, ExpressionSetOperations eso, boolean guard) {


		RelationReport rr = new RelationReport(relation);

		// count files
		long numFiles = mapping.getPaths(relation).size(); 
		rr.setNumFiles(numFiles);

		// count size
		long numInputBytes = mapping.getRelationSize(relation); 
		rr.setNumInputBytes(numInputBytes);

		// sample
		RelationReport rr1 = runSample(relation, group, eso, guard, 1);
		RelationReport rr2 = runSample(relation, group, eso, guard, 10);

		extrapolate(rr1,rr2,rr);

		return rr;
	}



	private RelationReport runSample(RelationSchema rs, Collection<GFExistentialExpression> group,
			ExpressionSetOperations eso, boolean guard, int factor) {

		RelationReport rr = new RelationReport(rs);

		try {
			// create new expressions


			// create algorithm
			MapAlgorithm algo;

			FakeMapper fm = new FakeMapper();
			Text t = new Text();

			if (guard) { // TODO make eso predicate
				Map1GuardMessageFactory fact = new Map1GuardMessageFactory(fm.context, settings, eso);
				fact.enableSampleCounting();
				algo = new Map1GuardAlgorithm(eso, fact);
			} else {
				Map1GuardedMessageFactory fact = new Map1GuardedMessageFactory(fm.context, settings, eso);
				fact.enableSampleCounting();
				algo = new Map1GuardedAlgorithm(eso,fact);

			}

			long totalBytesRead = 0;

			for (Path path: mapping.getPaths(rs)) {
				byte [][] rawbytes = Sampler.getRandomBlocks(path, SAMPLES_PER_FILE, SAMPLE_SIZE*factor);

				for (int i = 0; i < SAMPLES_PER_FILE; i++) {
					// read text lines
					try(LineReader l = new LineReader(new ByteArrayInputStream(rawbytes[i]))) {
						// skip first line, as it may be incomplete
						long bytesread = l.readLine(t); 

						long offset = 0;
						while (true) {
							// read next line
							bytesread = l.readLine(t); 
							if (bytesread <= 0)
								break;

							// if csv, wrap
							InputFormat format = mapping.getInputFormat(rs);

							String s = t.toString();
							if (format == InputFormat.CSV) {
								s = rs.getName() + "(" + s + ")";
							}

							// convert to tuple
							Tuple tuple = new Tuple(s);

							// feed to algorithm
							algo.run(tuple, offset);
							offset += bytesread;
						}

						// keep track of actual bytes read
						totalBytesRead += offset;
					}


				}
			}

			// get results from fake context
			// and extrapolate
			double ratio = 1; //rr.getNumInputBytes() / (double)totalBytesRead;

			rr.setNumInputBytes(totalBytesRead);
			rr.setEstInputTuples((long)(ratio * fm.context.getInputTuples()));
			rr.setEstIntermTuples((long) (ratio * fm.context.getOutputTuples()));
			rr.setEstIntermBytes((long) (ratio * fm.context.getOutputBytes()));
			rr.setEstIntermKeyBytes((long) (ratio * fm.context.getOutputKeyBytes()));
			rr.setEstIntermValueBytes((long) (ratio * fm.context.getOutputValueBytes()));

			// average size of fields
			// TODO use tuples for this




		} catch (SamplingException | AlgorithmInterruptedException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rr;


	}

	private void extrapolate(RelationReport rr1, RelationReport rr2,RelationReport goal) {
		try {
			LinearExtrapolator le = new LinearExtrapolator();
			for (Field f : RelationReport.class.getFields()) {
				
				if (f.getName().contains("est")) {
					le.loadValues(rr1.getNumInputBytes(), (long)f.get(rr1), rr2.getNumInputBytes(), (long)f.get(rr2));
					f.set(goal, (long)le.extrapolate(goal.getNumInputBytes()));
				}
			}
		} catch (IllegalArgumentException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
